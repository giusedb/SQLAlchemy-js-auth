from typing import Any

from sqlalchemy.orm import DeclarativeBase, DeclarativeMeta, Session

from .models import create_from_mixin, UserMixin, GroupMixin, RoleMixin, define_relation_tables, PermissionMixin


class Auth:
    """Public authorization class."""

    def __init__(self, model_base: DeclarativeMeta,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                 user_class:DeclarativeBase=None,
                 group_class:DeclarativeBase=None,
                 role_class:DeclarativeBase=None,
                 permission_class:DeclarativeBase=None,
                 context: 'jsalchemy_api.context.manager.ContextManager' = None) -> None:
        self.User = user_class or create_from_mixin(UserMixin, model_base)  # pylint: disable=invalid-name
        self.Group = group_class or create_from_mixin(GroupMixin, model_base)  # pylint: disable=invalid-name
        self.Role = role_class or create_from_mixin(RoleMixin, model_base)  # pylint: disable=invalid-name
        self.Permission = permission_class or create_from_mixin(PermissionMixin, model_base)  # pylint: disable=invalid-name
        self.context = context
        self.define_relation_tables(self, model_base.metadata)


    def join(self, user_id: int, group_id: int) -> None:
        """Join a user and a group."""

    def can(self, user: UserMixin, action: str, context: Any='global') -> bool:
        """Check whether the `user` can perform `action` on a `context`."""
        return True