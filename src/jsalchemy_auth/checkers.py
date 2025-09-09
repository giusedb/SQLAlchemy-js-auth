from typing import Set, List

from sqlalchemy.orm import DeclarativeBase

from jsalchemy_auth.traversers import treefy_paths, tree_traverse
from jsalchemy_auth.utils import to_context, Context


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

class PathPermission(PermissionChecker):
    def __init__(self,permission: str, *path: List[str], auth: "Auth"=None):
        """Check if the user has the permission for the object following the path."""
        self.permission = permission
        self.paths = treefy_paths(*path) or {}
        self.auth = auth

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
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

class GroupOwnerPermission(PermissionChecker):
    def __init__(self, on: str, auth: "Auth"=None):
        """check if the user id is the same as the object id following the path."""
        self.auth = auth
        self.path = on
        self.path_length = on.count(".") + 1

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """Check weather at least one of the roles are assigned to """
        group_ids = await self.auth._has_any_role(group_ids, role_ids)
        async for value in traverse(object, self.path, is_root=True, start=self.path_length):
            if set(value).intersection(group_ids):
                return True
        return False

class GlobalPermission(PermissionChecker):

    def __init__(self, permission: str, auth: "Auth"=None):
        self.auth = auth
        self.permission = permission

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        from jsalchemy_auth.auth import GLOBAL_CONTEXT
        if self.permission in await self.auth._global_permissions():
            return await self.auth._has_any_role(group_ids, role_ids)
        for group_id in group_ids:
            global_role_ids = await self.auth._contextual_roles(group_id, GLOBAL_CONTEXT)
            if global_role_ids.intersection(role_ids):
                return True
        return False

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

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        context = to_context(object)
        for checker in self.checkers:
            if await checker(group_ids, role_ids, context):
                return True
        return False

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

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
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

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        context = to_context(object)
        return not await self.checker(group_ids, role_ids, context)
