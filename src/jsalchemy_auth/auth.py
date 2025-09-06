from collections import namedtuple
from functools import reduce
from itertools import groupby
from operator import itemgetter
from typing import Type, Dict, List, Any, Optional, Union, Set, NamedTuple

from jsalchemy_web_context import db as session
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import String, Boolean, Integer, ForeignKey, Table, Column, select, update

from jsalchemy_web_context.caching import redis_cached_function
from .models import UserMixin, UserGroupMixin, RoleMixin, PermissionMixin, define_tables
from .checkers import PathPermission, GlobalPermission
from .utils import Context, to_context, inverted_properties


class PermissionGrantError(Exception):
    """Raised when a permission cannot be granted to a table in the context."""
    pass

GLOBAL_CONTEXT = Context(id=0, table='global_context')

class Auth:
    _all_paths: Dict[str, PathPermission] = {}

    def __init__(
            self,
            base_class: DeclarativeBase,
            propagation_schema: Dict[str, List[str]] = None,
            actions: Dict[str, Any] = None,
            user_model: Type[UserMixin] = None,
            group_model: Type[UserGroupMixin] = None,
            role_model: Type[RoleMixin] = None,
            permission_model: Type[PermissionMixin] = None
    ):
        self.propagation_schema = propagation_schema or {}
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

    async def _user_groups(self, user_id: int) -> List[int]:
        """Get the user groups for a user."""
        result = await session.execute(
            membership.select().where(
                (membership.c.user_id == user_id)
            )
        )
        return {row.usergroup_id for row in result.fetchall()}

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

    # @redis_cached_function()
    async def _perms_to_roles(self) -> Dict[int, Set[int]]:
        all = (await session.execute(select(role_permission.c.permission_id,
                                            role_permission.c.role_id))).all()
        return {p: set(map(itemgetter(1), group))
                for p, group in groupby(sorted(all), itemgetter(0))}

    # @redis_cached_function()
    async def _perm_name_ids(self) -> Dict[str, int]:
        """Return the full translation of permission names to ids."""
        return {row.name: row.id
                for row in await session.execute(
                    select(self.permission_model.name,
                           self.permission_model.id))}

    async def _resolve_permission(self, permission_name: str) -> Set[int]:
        """Find all role ids associated with a permission name."""
        name_ids = await self._perm_name_ids()
        return (await self._perms_to_roles())[name_ids[permission_name]]

    # @redis_cached_function()
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

    def _explode_partial_schema(self, model_class: Type[DeclarativeBase]):
        """Follow the schema provided and build all paths from a model class."""
        def explore(schema: Dict[str, List[str]]):
            for key, value in schema.items():
                yield key
                yield from explore(value)
        return list(explore(self.propagation_schema))

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

    async def can(self, user, action: str, context):
        """Checks if a user can perform an action on the context."""
        permission_name = action
        group_ids = await self._user_groups(user.id)
        role_ids = await self._resolve_permission(permission_name)
        context = to_context(context)

        if context.table not in self.actions:
            self.actions[context.table] = {}
        if action not in self.actions[context.table]:
            paths = self._explode_partial_schema(object.__class__)
            perm = GlobalPermission(action, auth=self) | PathPermission(action, auth=self, *paths)
            self.actions[context.table][action] = perm

        return await self.actions[context.table][action](group_ids, role_ids, context)

    async def has_permission(self, user: UserMixin, permission_name: str, context: Context | DeclarativeBase):
        """Checks if a user has the specified permission into a specific `context`."""
        role_ids = await self._resolve_permission(permission_name)
        user_groups = await self._user_groups(user.id)
        if isinstance(context, self.base_class):
            context = to_context(context)
        roles_ids = [await self._contextual_roles(group_id, context) for group_id in user_groups]
        valid_roles = reduce(set.union, filter(bool, roles_ids), set())
        return bool(role_ids.intersection(valid_roles))

    async def has_role(self, user, role_name: str):
        """Checks if a user has the specified role in any context."""
        # Get user groups
        user_groups = await self._get_user_groups(user)

        for group in user_groups:
            # Check if this group has the role (in any context)
            result = await session.execute(
                rolegrant.select().where(
                    (rolegrant.c.usergroup_id == group.id)
                )
            )

            grants = result.fetchall()
            for grant in grants:
                role = await self._get_role_by_id(grant.role_id)
                if role and role.name == role_name:
                    return True

        return False

    async def has_role_group(self, user_group, role_name: str):
        """Checks if a UserGroup has the specified role in any context."""
        # Check if this group has the role (in any context)
        result = await session.execute(
            rolegrant.select().where(
                (rolegrant.c.usergroup_id == user_group.id)
            )
        )

        grants = result.fetchall()
        for grant in grants:
            role = await self._get_role_by_id(grant.role_id)
            if role and role.name == role_name:
                return True

        return False

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

    async def _get_permission(self, name: str) -> Optional[PermissionMixin]:
        """Get a permission by name."""
        result = await session.execute(
            self.permission_model.__table__.select().where(
                self.permission_model.__table__.c.name == name
            )
        )
        return result.fetchone()

    async def _get_or_create_role(self, name: str) -> RoleMixin:
        """Get or create a role by name."""
        result = await session.execute(
            self.role_model.__table__.select().where(
                self.role_model.__table__.c.name == name
            )
        )
        role = result.fetchone()
        if not role:
            new_role = self.role_model(name=name)
            session.add(new_role)
            await session.flush()
            return new_role
        return role

    async def _get_role(self, name: str) -> Optional[RoleMixin]:
        """Get a role by name."""
        result = await session.execute(
            select(self.role_model)
            .where(self.role_model.name == name)
        )
        return result.scalar_one_or_none()

    async def _get_role_by_id(self, id: int) -> Optional[RoleMixin]:
        """Get a role by ID."""
        result = await session.execute(
            self.role_model.__table__.select().where(
                self.role_model.__table__.c.id == id
            )
        )
        return result.fetchone()
