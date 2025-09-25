"""
Permission checker implementations for authorization in JSAlchemy Auth.

This module provides various permission checkers that can be used to enforce
authorization rules on database objects. These checkers support path-based
permissions, ownership checks, group membership, and global permissions.
"""

from typing import Set, List

from sqlalchemy import Select, or_
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty

from jsalchemy_auth.traversers import treefy_paths, tree_traverse, traverse, class_traverse, all_paths, \
    aggregate_references
from jsalchemy_auth.utils import to_context, Context, invert_prop, ContextSet
from .models import UserMixin


class PermissionChecker:
    """
    Base class for all permission checkers.

    Permission checkers are used to determine whether a user has the necessary
    permissions to access or modify database objects. They support various
    operations including joins and where clause generation for database queries.

    Attributes:
        auth: Reference to the authentication system instance
    """

    auth: "Auth" = None

    def __or__(self, other):
        """
        Combine two permission checkers with OR logic.

        Args:
            other: Another PermissionChecker instance

        Returns:
            Or: A new Or permission checker combining self and other
        """
        return Or(self, other)

    def __and__(self, other):
        """
        Combine two permission checkers with AND logic.

        Args:
            other: Another PermissionChecker instance

        Returns:
            And: A new And permission checker combining self and other
        """
        return And(self, other)

    def __invert__(self):
        """
        Negate a permission checker with NOT logic.

        Returns:
            Not: A new Not permission checker wrapping self
        """
        return Not(self)

    def __repr__(self):
        """Return string representation of the permission checker."""
        return f"- [{self.__class__.__name__}] -"

    async def joins(self, group_ids: Set[int], target: DeclarativeBase, permission_name: str = 'read') -> List[
        RelationshipProperty]:
        """
        Generate the joins required for checking permissions.

        Args:
            group_ids: Set of group IDs that the user belongs to
            target: The target database model to check permissions for
            permission_name: The name of the permission to check (default: 'read')

        Returns:
            List[RelationshipProperty]: List of relationship properties to join

        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase) -> List:
        """
        Generate WHERE clause conditions for checking permissions.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            target: The target database model to check permissions for
            permission_name: The name of the permission to check (default: 'read')

        Returns:
            List: SQL WHERE clause conditions

        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError


class Path(PermissionChecker):
    """
    Permission checker that validates permissions based on path traversal.

    This checker checks if a user has the required permission for an object
    by traversing paths from the target object to find relevant contexts.

    Attributes:
        permission: The permission string to check for
        paths: Treeified path structures for traversal
        auth: Reference to the authentication system instance
    """

    def __init__(self, permission: str, *path: List[str], auth: "Auth" = None):
        """
        Initialize a Path permission checker.

        Args:
            permission: The permission string to check for
            *path: Path components that define the traversal path
            auth: Reference to the authentication system instance
        """
        self.permission = permission
        self.paths = treefy_paths(*path) or {}
        self.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if user has permission for the object by traversing paths.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check permissions for

        Returns:
            bool: True if user has permission, False otherwise
        """
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
        """
        Generate joins needed for path-based permission checking.

        Args:
            group_ids: Set of group IDs that the user belongs to
            target: The target database model to check permissions for

        Returns:
            List[RelationshipProperty]: List of relationship properties to join
        """
        permitted_contexts = await self.auth.contexts_by_permission(group_ids, self.permission)
        if not permitted_contexts:
            return [False]
        ret = []
        if permitted_contexts:
            models = {context.model for context in permitted_contexts}
            for path in all_paths(self.paths):
                # traverse all paths to find the tables where permissions are assigned
                partial_path = []
                rec_join = None
                for prop in class_traverse(target, path):
                    if prop.target in prop.parent.tables:
                        rec_join = partial_path.copy()
                    partial_path.append(prop)
                    if prop.entity.class_ in models:
                        if rec_join is not None:
                            for p in rec_join:
                                if p not in ret:
                                    ret.append(p)
                            break
                        for p in partial_path:
                            if p not in ret:
                                ret.append(p)
        return ret

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase) -> List:
        """
        Generate WHERE clause conditions for path-based permission checking.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            target: The target database model to check permissions for
            permission_name: The name of the permission to check (default: 'read')

        Returns:
            List: SQL WHERE clause conditions
        """
        # Get permitted models and their IDs
        permitted = {c.model: c.ids for c in await self.auth.contexts_by_permission(group_ids, self.permission)}

        # Build the filter conditions
        items = []

        # Add conditions for all paths and their targets
        for path in all_paths(self.paths):
            overjoin = None
            overpath = None
            for step in class_traverse(target, path):
                if step.target in step.parent.tables:
                    overjoin = []
                    overpath = []
                if overpath is not None:
                    overpath.append(step)
                model = self.auth.to_class(step.target)
                if model in permitted:
                    if overjoin is None:
                        items.append(ContextSet(model, permitted[model]))
                    else:
                        overjoin.append((overpath.copy(), permitted[model]))
        if overjoin:
            for path, ids in overjoin:
                overpath = '.'.join(invert_prop(x).key for x in reversed(path))
                model = path[-1].entity.class_
                context = ContextSet(model, ids)
                async for context in traverse(context, overpath):
                    items.append(context)

        # Add condition for target model itself
        if target in permitted:
            items.append(ContextSet(target, permitted[target]))

        contexts = aggregate_references(items)
        if contexts:
            return or_(*(context.model.id.in_(context.ids) for context in contexts))
        return False


class Owner(PermissionChecker):
    """
    Permission checker that validates ownership of database objects.

    This checker checks if a user owns an object by comparing the user's ID
    with the object's owner field along a specified path.

    Attributes:
        auth: Reference to the authentication system instance
        path: Path string that defines where to find the owner field
        path_length: Number of path components in the path
    """

    def __init__(self, on: str, auth: "Auth" = None):
        """
        Initialize an Owner permission checker.

        Args:
            on: Path string that defines where to find the owner field
            auth: Reference to the authentication system instance
        """
        self.auth = auth
        self.path = on
        self.path_length = on.count(".") + 1

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if user owns the object by traversing the path.

        Args:
            user: The user to check ownership for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check ownership for

        Returns:
            bool: True if user owns the object, False otherwise
        """
        async for value in traverse(object, self.path, start=self.path_length):
            if user.id in value:
                return True
        return False

    async def joins(self, group_ids: Set[int], target: DeclarativeBase) -> List[RelationshipProperty]:
        """
        Generate joins needed for owner-based permission checking.

        Args:
            group_ids: Set of group IDs that the user belongs to
            target: The target database model to check permissions for

        Returns:
            List[RelationshipProperty]: List of relationship properties to join
        """
        return [prop for prop in class_traverse(target, '.'.join(self.path.split('.')[:-1]))]

    def _where_condition(self, attribute, user, group_ids):
        """
        Generate the WHERE condition for owner checks.

        Args:
            attribute: The database attribute to compare
            user: The user to check ownership for
            group_ids: Set of group IDs that the user belongs to

        Returns:
            SQL condition: The WHERE condition for owner check
        """
        return attribute == user.id

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase) -> List:
        """
        Generate WHERE clause conditions for owner-based permission checking.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            target: The target database model to check permissions for
            permission_name: The name of the permission to check (default: 'read')

        Returns:
            List: SQL WHERE clause conditions
        """
        return self._where_condition(
            tuple(class_traverse(target, self.path))[-1].class_attribute,
            user, group_ids)


class Group(Owner):
    """
    Permission checker that validates group membership for database objects.

    This checker checks if a user belongs to one of the groups specified in
    an object field along a specified path.

    Attributes:
        auth: Reference to the authentication system instance
        path: Path string that defines where to find the group field
        path_length: Number of path components in the path
    """

    def __init__(self, on: str, auth: "Auth" = None):
        """
        Initialize a Group permission checker.

        Args:
            on: Path string that defines where to find the group field
            auth: Reference to the authentication system instance
        """
        self.auth = auth
        self.path = on
        self.path_length = on.count(".") + 1

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if user belongs to one of the groups in the object.

        Args:
            user: The user to check group membership for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check group membership for

        Returns:
            bool: True if user belongs to a relevant group, False otherwise
        """
        async for value in traverse(object, self.path, start=self.path_length):
            if set(value).intersection(group_ids):
                return True
        return False

    def _where_condition(self, attribute, user, group_ids):
        """
        Generate the WHERE condition for group checks.

        Args:
            attribute: The database attribute to compare
            user: The user to check group membership for
            group_ids: Set of group IDs that the user belongs to

        Returns:
            SQL condition: The WHERE condition for group check
        """
        return attribute.in_(group_ids)


class Global(PermissionChecker):
    """
    Permission checker for global permissions.

    This checker validates whether a user has global permissions that allow
    access to all resources regardless of path or ownership.

    Attributes:
        auth: Reference to the authentication system instance
        permission: The global permission string to check for
    """

    def __init__(self, permission: str, auth: "Auth" = None):
        """
        Initialize a Global permission checker.

        Args:
            permission: The global permission string to check for
            auth: Reference to the authentication system instance
        """
        self.auth = auth
        self.permission = permission

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if user has global permission.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check permissions for (unused)

        Returns:
            bool: True if user has global permission, False otherwise
        """
        from jsalchemy_auth.auth import GLOBAL_CONTEXT
        if self.permission in await self.auth._global_permissions():
            return await self.auth._has_any_role(group_ids, role_ids)
        for group_id in group_ids:
            global_role_ids = await self.auth._contextual_roles(group_id, GLOBAL_CONTEXT)
            if global_role_ids.intersection(role_ids):
                return True
        return False

    async def joins(self, group_ids: Set[int], target: DeclarativeBase) -> List[RelationshipProperty]:
        """
        Generate joins needed for global permission checking.

        Args:
            group_ids: The group ids that user is member of
            query: The SQL query to generate joins for

        Returns:
            List: List containing None (no joins needed)
        """
        role_ids = await self.auth._resolve_permission(self.permission)
        if await self(None, group_ids, role_ids, target):
            return [True]
        return []

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase):
        """
        Generate WHERE clause conditions for global permission checking.

        Args:
            user: The user to check permissions for
            query: The SQL query to generate conditions for
            permission_name: The name of the permission to check (default: 'read')

        Returns:
            None: No WHERE conditions needed for global permissions
        """
        return None


class Or(PermissionChecker):
    """
    Permission checker that combines multiple checkers with OR logic.

    This checker returns True if ANY of the wrapped permission checkers return True.

    Attributes:
        checkers: List of PermissionChecker instances to combine with OR logic
    """

    def __init__(self, *permission_checker):
        """
        Initialize an OR permission checker.

        Args:
            *permission_checker: Variable number of PermissionChecker instances
        """
        self.checkers = permission_checker

    @property
    def auth(self):
        """Get the authentication system instance from the first checker."""
        return self.checkers[0].auth

    @auth.setter
    def auth(self, auth: "Auth"):
        """
        Set the authentication system instance for all checkers.

        Args:
            auth: The authentication system instance to set
        """
        for checker in self.checkers:
            checker.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if any of the wrapped checkers return True.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check permissions for

        Returns:
            bool: True if any checker returns True, False otherwise
        """
        context = to_context(object)
        for checker in self.checkers:
            if await checker(user, group_ids, role_ids, context):
                return True
        return False

    async def joins(self, user: UserMixin, target: DeclarativeBase, permission_name: str = 'read'):
        """
        Generate joins needed for OR permission checking.

        Args:
            user: The user to check permissions for
            target: The target database model to check permissions for
            permission_name: The name of the permission to check (default: 'read')

        Returns:
            List[RelationshipProperty]: List of relationship properties to join
        """
        return [prop for checker in self.checkers for prop in await checker.joins(user, target)]

    async def where(self, user: UserMixin, group_ids: Set[int], target: DeclarativeBase) -> List:
        conditions = [await checker.where(user, group_ids, target) for checker in self.checkers]
        conditions = [cond for cond in conditions if cond is not None]
        if any(condition is True for condition in conditions):
            return True
        if len(conditions) == 1:
            return conditions[0]
        if len(conditions) > 1:
            return or_(*conditions)
        return []


class And(PermissionChecker):
    """
    Permission checker that combines multiple checkers with AND logic.

    This checker returns True only if ALL of the wrapped permission checkers return True.

    Attributes:
        checkers: List of PermissionChecker instances to combine with AND logic
    """

    def __init__(self, *permission_checker):
        """
        Initialize an AND permission checker.

        Args:
            *permission_checker: Variable number of PermissionChecker instances
        """
        self.checkers = permission_checker

    @property
    def auth(self):
        """Get the authentication system instance from the first checker."""
        return self.checkers[0].auth

    @auth.setter
    def auth(self, auth: "Auth"):
        """
        Set the authentication system instance for all checkers.

        Args:
            auth: The authentication system instance to set
        """
        for checker in self.checkers:
            checker.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if all of the wrapped checkers return True.

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check permissions for

        Returns:
            bool: True if all checkers return True, False otherwise
        """
        context = to_context(object)
        for checker in self.checkers:
            if not await checker(group_ids, role_ids, context):
                return False
        return True


class Not(PermissionChecker):
    """
    Permission checker that negates another permission checker with NOT logic.

    This checker returns the logical inverse of a wrapped permission checker.

    Attributes:
        checker: The PermissionChecker instance to negate
    """

    def __init__(self, permission_checker: PermissionChecker):
        """
        Initialize a NOT permission checker.

        Args:
            permission_checker: The PermissionChecker instance to negate
        """
        self.checker = permission_checker

    @property
    def auth(self):
        """Get the authentication system instance from the wrapped checker."""
        return self.checker.auth

    @auth.setter
    def auth(self, auth: "Auth"):
        """
        Set the authentication system instance for the wrapped checker.

        Args:
            auth: The authentication system instance to set
        """
        self.checker.auth = auth

    async def __call__(self, user: UserMixin, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """
        Check if the wrapped checker returns False (logical NOT).

        Args:
            user: The user to check permissions for
            group_ids: Set of group IDs that the user belongs to
            role_ids: Set of role IDs that the user has
            object: The database object to check permissions for

        Returns:
            bool: True if the wrapped checker returns False, False otherwise
        """
        context = to_context(object)
        return not await self.checker(group_ids, role_ids, context)
