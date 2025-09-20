from collections import namedtuple
from functools import reduce, partial
from itertools import groupby
from operator import itemgetter
from typing import Type, Dict, List, Any, Optional, Union, Set, NamedTuple

from jsalchemy_web_context import db as session
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import String, Boolean, Integer, ForeignKey, Table, Column, select, update, Select

from jsalchemy_web_context.cache import redis_cache, request_cache
from .utils import Context, to_context, inverted_properties, ContextSet, table_to_class, get_target_table
from .models import UserMixin, UserGroupMixin, RoleMixin, PermissionMixin, define_tables
from .checkers import Path, Global


class PermissionGrantError(Exception):
    """Raised when a permission cannot be granted to a table in the context."""
    pass

GLOBAL_CONTEXT = Context(id=0, model=None)

class Auth:
    _all_paths: Dict[str, Path] = {}
    _propagation_schema: Dict[str, List[str]] = {}
    _inv_propagation_schema: Dict[str, List[str]] = {}

    def __init__(
            self,
            base_class: DeclarativeBase,
            propagation_schema: Dict[str, List[str]] = None,
            actions: Dict[str, Any] = None,
            user_model: Type[UserMixin] = None,
            group_model: Type[UserGroupMixin] = None,
            role_model: Type[RoleMixin] = None,
            permission_model: Type[PermissionMixin] = None,

    ):
        self.actions = actions or {}
        self.user_model = user_model
        self.group_model = group_model
        self.role_model = role_model
        self.permission_model = permission_model
        self.base_class = base_class
        self._define_tables(base_class)
        if actions:
            for action in actions.values():
                for permission in action.values():
                    permission.auth = self
        self.propagation_schema = propagation_schema or {}
        self.to_class = partial(table_to_class, self.base_class)

    @property
    def propagation_schema(self):
        return self._permission_schema

    @propagation_schema.setter
    def propagation_schema(self, value):
        self._permission_schema = value or {}
        self._inv_propagation_schema = inverted_properties(value or {}, self.base_class.registry)

    @property
    def inv_propagation_schema(self):
        return self._inv_propagation_schema


    def _define_tables(self, Base: DeclarativeBase):
        """Create all database tables for the models."""
        global membership, rolegrant, role_permission
        from jsalchemy_auth.models import UserMixin, UserGroupMixin, RoleMixin, PermissionMixin
        for model, mixin in [('user_model', UserMixin), ('group_model', UserGroupMixin),
                             ('role_model', RoleMixin), ('permission_model', PermissionMixin)]:
            if not getattr(self, model):
                class_name = ''.join(map(str.capitalize, model.split("_")[:-1]))
                if 'id' not in getattr(mixin, '__annotations__'):
                    setattr(mixin, 'id', mapped_column(Integer, primary_key=True))
                setattr(self, model, type(class_name, (mixin, Base), {'__tablename__': class_name.lower() + 's'}))

        # Create all tables in the database
        role_permission, rolegrant, membership = define_tables(
            Base, self.user_model, self.group_model, self.role_model, self.permission_model)

    async def _get_role(self, name: str) -> Optional[RoleMixin]:
        """Get a role by name."""
        result = await session.execute(
            select(self.role_model)
            .where(self.role_model.name == name)
        )
        return result.scalar_one_or_none()

    async def _get_or_create_permission(self, name: str):
        """Get or create a permission by name."""
        result = await session.execute(
            self.permission_model.__table__.select().where(
                self.permission_model.__table__.c.name == name
            )
        )
        perm = result.fetchone()
        if not perm:
            new_perm = self.permission_model(name=name)
            session.add(new_perm)
            await session.flush()
            return new_perm
        return perm

    async def _get_or_create_role(self, name: str) -> RoleMixin:
        """Get or create a role by name."""
        role = await self._get_role(name)
        if not role:
            new_role = self.role_model(name=name)
            session.add(new_role)
            await session.flush()
            return new_role
        return role

    async def _user_groups(self, user_id: int) -> List[int]:
        """Get the user groups for a user."""
        result = await session.execute(
            membership.select().where(
                (membership.c.user_id == user_id)
            )
        )
        return {row.usergroup_id for row in result.fetchall()}

    @request_cache('group_id', 'context.id', 'context.table')
    @redis_cache('group_id', 'context.id', 'context.table')
    async def _contextual_roles(self, group_id: int, context: Context) -> Set[int]:
        """Get the Set of Role.ids for a set of groups identified by their ids."""
        result = await session.execute(
            select(rolegrant.c.role_id).where(
                (rolegrant.c.usergroup_id == group_id) &
                (rolegrant.c.context_id == context.id) &
                (rolegrant.c.context_table == context.table)
            )
        )
        return set(result.scalars())

    @request_cache()
    @redis_cache()
    async def _perms_to_roles(self) -> Dict[int, Set[int]]:
        all = (await session.execute(
            select(self.permission_model.name, role_permission.c.role_id)
            .join(role_permission, self.permission_model.id == role_permission.c.permission_id))).all()
        return {p: set(map(itemgetter(1), group))
                for p, group in groupby(sorted(all), itemgetter(0))}

    @request_cache()
    @redis_cache()
    async def _perm_name_ids(self) -> Dict[str, int]:
        """Return the full translation of permission names to ids."""
        return {row.name: row.id
                for row in await session.execute(
                    select(self.permission_model.name,
                           self.permission_model.id))}

    async def _resolve_permission(self, permission_name: str) -> Set[int]:
        """Find all role ids associated with a permission name."""
        ref = await self._perms_to_roles()
        if permission_name not in ref:
            return set()
        return ref[permission_name]

    @request_cache()
    @redis_cache()
    async def _global_permissions(self) -> Set[str]:
        """Find all global permissions and return their names."""
        result = await session.execute(
            select(self.permission_model.name).where(
                self.permission_model.is_global == True
            )
        )
        return set(result.scalars())

    async def _has_any_role(self, group_ids: Set[int], role_ids: Set[int]) -> bool:
        """Check if any of the group_ids have any of the role_ids."""
        return bool((await session.execute(
            rolegrant.select().where(
                (rolegrant.c.usergroup_id.in_(group_ids)) &
                (rolegrant.c.role_id.in_(role_ids))
            )
        )).scalar())

    @property
    def inverted_schema(self):
        """Return the inverted schema."""
        return inverted_properties(self.propagation_schema)

    def _explode_partial_schema(self, table: str, depth: int = 0) -> Set[str]:
        """Follow the schema provided and build all paths from a model class."""

        def tree_explore(node: str) -> Set[str]:
            """
            Return all dotted paths that can be formed from ``node`` by following the
            relations defined in ``schema``.
            """
            nonlocal schema, mappers
            if node not in schema:
                return set()

            paths: Set[str] = set()
            for child in schema[node]:
                # the direct edge
                paths.add(child)
                child_class = mappers[node].relationships[child].entity.class_.__name__
                # recursively extend from the child
                paths.update({f"{child}.{sub}" for sub in tree_explore(child_class)})

            return paths
        mappers = {m.class_.__name__: m for m in self.base_class.registry.mappers}
        schema = self.inv_propagation_schema
        return tree_explore(table)

    async def set_permission_global(self, is_global: bool, *permission_name: List[str]):
        """Set a permission to be global."""
        existing_permissions = (await session.execute(
            select(self.permission_model).where(
                (self.permission_model.name.in_(permission_name))
            )
        )).scalars().all()
        for permission in existing_permissions:
            permission.is_global = is_global
        for name in set(permission_name).difference({p.name for p in existing_permissions}):
            session.add(self.permission_model(name=name, is_global=is_global))
        await session.flush()
        await self._global_permissions.discard_all()

    async def assign(self, role_name: str, *permission_name: List[str]):
        """Assigns a permission to a role."""
        # Find or create the permission
        role = await self._get_or_create_role(role_name)

        # Find or create roles and associate them with the permission
        for permission_n in permission_name:
            permission = await self._get_or_create_permission(permission_n)
            # Check if this association already exists
            existing_assoc = await session.execute(
                role_permission.select().where(
                    (role_permission.c.role_id == role.id) &
                    (role_permission.c.permission_id == permission.id)
                )
            )

            if not existing_assoc.fetchone():
                await session.execute(
                    role_permission.insert().values(
                        role_id=role.id,
                        permission_id=permission.id
                    )
                )
        await self._perms_to_roles.discard_all()

    async def unassign(self, role_name: str, pemrission_names: List[str]) -> bool:
        """Removes a permission from a role."""
        # Find the permission
        permission_ids = await session.execute(
            role_permission.select(role_permission.c.permission_id).where(
                (role_permission.c.permission_name.in_(pemrission_names))
            )
        ).scalars()

        # Find the role
        role = await self._get_role(role_name)
        if not role:
            return

        # Remove the association
        await session.execute(
            role_permission.delete().where(
                (role_permission.c.role_id == role.id) &
                (role_permission.c.permission_id in permission_ids)
            )
        )

    async def _get_user_group(self, user: UserMixin) -> UserGroupMixin:
        private_groups = {g for g in await user.awaitable_attrs.memberships if g.is_personal and g.owner_id == user.id}
        if not private_groups:
            user_group = self.group_model(owner_id=user.id, is_personal=True, name=f'private:{user.id}')
            (await user.awaitable_attrs.memberships).append(user_group)
            session.add(user_group)
            await session.flush()
        else:
            user_group = next(iter(private_groups))
        return user_group

    async def grant(self, user_group: UserGroupMixin | UserMixin, role_name: str, context) -> bool:
        """Grants a role to a UserGroup in the context of a specific database record."""
        # Validate that the role can be granted to the table used in the context
        context = to_context(context)
        if isinstance(user_group, UserMixin):
            user_group = await self._get_user_group(user_group)

        # Get the role
        role = await self._get_role(role_name)
        if not role:
            raise PermissionGrantError(f"Role {role_name} does not exist")

        # Check if the role's associated tables include the context table
        if role.tables and context.table not in role.tables.split(','):
            raise PermissionGrantError(
                f"Role {role_name} cannot be granted to table {context.table}"
            )

        # Check if this grant already exists
        existing_grant = await session.execute(
            rolegrant.select().where(
                (rolegrant.c.usergroup_id == user_group.id) &
                (rolegrant.c.role_id == role.id) &
                (rolegrant.c.context_id == context.id) &
                (rolegrant.c.context_table == context.table)
            )
        )

        if not existing_grant.fetchone():
            await session.execute(
                rolegrant.insert().values(
                    usergroup_id=user_group.id,
                    role_id=role.id,
                    context_id=context.id,
                    context_table=context.table,
                )
            )
            await self._contextual_roles.discard(self, user_group.id, context)
            # await self.contexts_by_permission.discard(self, user_group.id, context)
            return True
        return False

    async def revoke(self, user_group, role_name: str, context: Context):
        """Revokes a role from a UserGroup in the context of a specific database record."""
        # Get the role
        role = await self._get_role(role_name)
        if not role:
            return

        # Remove the grant
        await session.execute(
            rolegrant.delete().where(
                (rolegrant.c.usergroup_id == user_group.id) &
                (rolegrant.c.role_id == role.id) &
                (rolegrant.c.context_id == context.id) &
                (rolegrant.c.context_table == context.table)
            )
        )
        await self._contextual_roles.discard(self, user_group.id, context)
        # await self.contexts_by_permission.discard(self, user_group.id, context)

    def _action_checker(self, action: str, model_name: str):
        """find the checker for the action onto the context."""
        if model_name not in self.actions:
            self.actions[model_name] = {}
        if action not in self.actions[model_name]:
            paths = self._explode_partial_schema(model_name)
            perm = Global(action, auth=self) | Path(action, auth=self, *paths)
            self.actions[model_name][action] = perm
        return self.actions[model_name][action]

    async def can(self, user, action: str, context):
        """Checks if a user can perform an action on the context."""
        permission_name = action
        group_ids = await self._user_groups(user.id)
        role_ids = await self._resolve_permission(permission_name)
        context = to_context(context)

        return await self._action_checker(action, context.model.__name__)(user, group_ids, role_ids, context)

    async def has_permission(self, user: UserMixin, permission_name: str, context: Context | DeclarativeBase):
        """Checks if a user has the specified permission into a specific `context`."""
        role_ids = await self._resolve_permission(permission_name)
        user_groups = await self._user_groups(user.id)
        if isinstance(context, self.base_class):
            context = to_context(context)
        roles_ids = [await self._contextual_roles(group_id, context) for group_id in user_groups]
        valid_roles = reduce(set.union, filter(bool, roles_ids), set())
        return bool(role_ids.intersection(valid_roles))

    async def contexts_by_permission(self, user: UserMixin | Set[int],
                                     permission: str,) -> Set[ContextSet]:
        """Find all contexts where the user has a specified permission."""
        if isinstance(user, UserMixin):
            group_ids = await self._user_groups(user.id)
        elif isinstance(user, set):
            group_ids = user
        else:
            raise ValueError("user must be a UserMixin or a set of group ids")

        role_ids = await self._resolve_permission(permission)

        if not group_ids or not role_ids:
            return []

        result = await session.execute(
            select(rolegrant.c.context_table, rolegrant.c.context_id)
            .where(
                (rolegrant.c.usergroup_id.in_(group_ids)) &
                (rolegrant.c.role_id.in_(role_ids)) &
                (rolegrant.c.context_table != 'global')
            )
        )

        return {ContextSet(self.to_class(table), tuple(map(itemgetter(1), grp)))
                for table, grp in groupby(sorted(result.fetchall()), itemgetter(0))}

    async def object_with_permission(self, user: UserMixin, permission: str):
        """finds every object the user has the permission to."""
        contexts = await self.contexts_by_permission(user, permission)
        ret = set()
        for context in contexts:
            if isinstance(context, ContextSet):
                ret.update((await session.execute(
                    select(context.model)
                    .where(context.model.id.in_(context.ids)))).scalars().all())
            else:
                ret.add((await session.
                    select(context.model)
                    .where(context.model.id == context.id)).scalar_one_or_none())
        return ret

    async def accessible_query(self, user: UserMixin, query: Select, action: str='read', false_=None):
        """Returns a query filtered by the user's permissions."""
        table = get_target_table(query)
        target = self.to_class(table)
        checker = self._action_checker(action, target.__name__)
        group_ids = await self._user_groups(user.id)
        joins = [join for join in await checker.joins(group_ids, target)]
        if not joins:
            return query.where(False)
        if None in joins:
            return query
        for prop in joins:
            query = query.outerjoin(prop.class_attribute)
        return query.filter(await checker.where(user, group_ids, target))
