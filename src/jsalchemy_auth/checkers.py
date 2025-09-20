from typing import Set, List

from sqlalchemy import Select, or_
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty

from jsalchemy_auth.traversers import treefy_paths, tree_traverse, traverse, class_traverse, all_paths
from jsalchemy_auth.utils import to_context, Context
from .models import UserMixin

class PermissionChecker:
    auth: "Auth" = None

    def __or__(self, other):
        return OrPermission(self, other)

    def __and__(self, other):
        return AndPermission(self, other)

    def __invert__(self):
        return NotPermission(self)

    def __repr__(self):
        return f"- [{self.__class__.__name__}] -"

    async def joins(self, group_ids: Set[int], target: DeclarativeBase , permission_name: str='read') -> List[RelationshipProperty]:
        """generate the joins from the permission that the user has."""
        raise NotImplementedError

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase , permission_name: str='read') -> List:
        """add the where clause to the ``query`` to check the permission that the ``user`` has."""
        raise NotImplementedError

class PathPermission(PermissionChecker):
    def __init__(self,permission: str, *path: List[str], auth: "Auth"=None):
        """Check if the user has the permission for the object following the path."""
        self.permission = permission
        self.paths = treefy_paths(*path) or {}
        self.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """Check weather at least one of the roles are assigned to """
        async for contexts in tree_traverse(object, self.paths, is_root=True):
            if isinstance(contexts, Context):
                contexts = [contexts]
            for context in contexts:
                for group_id in group_ids:
                    context_role_ids = await self.auth._contextual_roles(group_id, context)
                    if context_role_ids.intersection(role_ids):
                        return True
        return False

    async def joins(self, group_ids: Set[int], target: DeclarativeBase) -> List[RelationshipProperty]:
        permitted_contexts = await self.auth.contexts_by_permission(group_ids, self.permission)
        ret = []
        if permitted_contexts:
            models = {context.model for context in permitted_contexts}
            for path in all_paths(self.paths):
                # traverse all paths to find the tables where permissions are assigned
                partial_path = []
                for prop in class_traverse(target, path):
                    partial_path.append(prop)
                    if prop.entity.class_ in models:
                        for p in partial_path:
                            if p not in ret:
                                ret.append(p)
        return ret

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase, permission_name: str = 'read') -> List:
        permitted = {c.model: c.ids for c in await self.auth.contexts_by_permission(group_ids, self.permission)}
        items = [model.id.in_(permitted[model])
            for path in all_paths(self.paths)
                for model in (
                    self.auth.to_class(a.target)
                    for a in class_traverse(target, path))
                if model in permitted]
        if target in permitted:
            items.append(target.id.in_(permitted[target]))
        if items:
            return or_(*items)
        return []

class OwnerPermission(PermissionChecker):
    def __init__(self, on: str, auth: "Auth"=None):
        """check if the user id is the same as the object id following the path."""
        self.auth = auth
        self.path = on
        self.path_length = on.count(".") + 1

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """Check weather at least one of the roles are assigned to """
        async for value in traverse(object, self.path, start=self.path_length):
            if user.id in value:
                return True
        return False

    async def joins(self, group_ids: Set[int], target: DeclarativeBase ) -> List[RelationshipProperty]:
        return [prop for prop in class_traverse(target, '.'.join(self.path.split('.')[:-1]))]

    def _where_condition(self, attribute, user, group_ids):
        return attribute == user.id

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase , permission_name: str='read') -> List:
        return self._where_condition(
            tuple(class_traverse(target, self.path))[-1].class_attribute,
            user, group_ids)

class GroupOwnerPermission(OwnerPermission):
    def __init__(self, on: str, auth: "Auth"=None):
        """check if the user id is the same as the object id following the path."""
        self.auth = auth
        self.path = on
        self.path_length = on.count(".") + 1

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """Check weather at least one of the roles are assigned to """
        async for value in traverse(object, self.path, start=self.path_length):
            if set(value).intersection(group_ids):
                return True
        return False

    def _where_condition(self, attribute, user, group_ids):
        return attribute.in_(group_ids)

class GlobalPermission(PermissionChecker):

    def __init__(self, permission: str, auth: "Auth"=None):
        self.auth = auth
        self.permission = permission

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        from jsalchemy_auth.auth import GLOBAL_CONTEXT
        if self.permission in await self.auth._global_permissions():
            return await self.auth._has_any_role(group_ids, role_ids)
        for group_id in group_ids:
            global_role_ids = await self.auth._contextual_roles(group_id, GLOBAL_CONTEXT)
            if global_role_ids.intersection(role_ids):
                return True
        return False

    async def joins(self, user: UserMixin, query: Select):
        return [None]

    def where(self, user: UserMixin, query: Select, permission_name: str='read'):
        return None

class OrPermission(PermissionChecker):
    def __init__(self, *permission_checker):
        self.checkers = permission_checker

    @property
    def auth(self):
        return self.checkers[0].auth

    @auth.setter
    def auth(self, auth: "Auth"):
        for checker in self.checkers:
            checker.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        context = to_context(object)
        for checker in self.checkers:
            if await checker(user, group_ids, role_ids, context):
                return True
        return False

    async def joins(self, user: UserMixin, target: DeclarativeBase, permission_name: str='read'):
        return [prop for checker in self.checkers for prop in await checker.joins(user, target)]

class AndPermission(PermissionChecker):
    def __init__(self, *permission_checker):
        self.checkers = permission_checker

    @property
    def auth(self):
        return self.checkers[0].auth

    @auth.setter
    def auth(self, auth: "Auth"):
        for checker in self.checkers:
            checker.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        context = to_context(object)
        for checker in self.checkers:
            if not await checker(group_ids, role_ids, context):
                return False
        return True

class NotPermission(PermissionChecker):
    def __init__(self, permission_checker: PermissionChecker):
        self.checker = permission_checker

    @property
    def auth(self):
        return self.checker.auth

    @auth.setter
    def auth(self, auth: "Auth"):
        self.checker.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        context = to_context(object)
        return not await self.checker(group_ids, role_ids, context)
