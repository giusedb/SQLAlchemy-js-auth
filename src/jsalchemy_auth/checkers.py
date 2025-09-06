from typing import Set, List

from sqlalchemy.orm import DeclarativeBase

from jsalchemy_auth.traversors import treefy_paths, tree_traverse
from jsalchemy_auth.utils import to_context


class PermissionChecker:
    auth: "Auth" = None

    def __or__(self, other):
        return OrPermission(self, other)

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
        async for context in tree_traverse(object, self.paths):
            for group_id in group_ids:
                context_role_ids = await self.auth._contextual_roles(group_id, context)
                if context_role_ids.intersection(role_ids):
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

