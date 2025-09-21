"""
Database models for user management system with roles, permissions, and groups.

This module defines SQLAlchemy declarative base classes and mixins for building
a flexible role-based access control (RBAC) system with support for user groups,
roles, and permissions.
"""

from typing import List, Optional
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, declared_attr, relationship
from sqlalchemy import String, Boolean, Integer, ForeignKey, Table, Column
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine


# Define the base mixin classes
class UserMixin:
    """
    Mixin for user model.

    This class serves as a base for defining user models in the system.
    It provides common attributes and methods that can be extended or
    overridden by concrete implementations.
    """


class UserGroupMixin:
    """
    Mixin for user group model.

    Defines the structure and relationships for user groups in the system.
    User groups can contain users, have roles assigned to them, and can
    be owned by specific users.

    Attributes:
        name (Mapped[str]): Unique name identifier for the user group.
        is_personal (Mapped[bool]): Flag indicating if this is a personal group.
    """

    name: Mapped[str] = mapped_column(String(150), unique=True)
    is_personal: Mapped[bool] = mapped_column(Boolean, default=False)


class RoleMixin:
    """
    Mixin for role model.

    Defines the structure and relationships for roles in the system.
    Roles can be assigned to user groups and granted permissions.

    Attributes:
        name (Mapped[str]): Unique name identifier for the role.
        is_global (Mapped[bool]): Flag indicating if this is a global role.
        tables (Mapped[Optional[str]]): Comma-separated list of tables this role applies to.
    """

    name: Mapped[str] = mapped_column(String(150), unique=True)
    is_global: Mapped[bool] = mapped_column(Boolean, default=False)
    tables: Mapped[Optional[str]] = mapped_column(String(255))  # Comma-separated list of tables


class PermissionMixin:
    """
    Mixin for permission model.

    Defines the structure and relationships for permissions in the system.
    Permissions can be granted to roles and control access to specific resources.

    Attributes:
        name (Mapped[str]): Unique name identifier for the permission.
        is_global (Mapped[bool]): Flag indicating if this is a global permission.
    """

    name: Mapped[str] = mapped_column(String(150), unique=True)
    is_global: Mapped[bool] = mapped_column(Boolean, default=False)


# Association tables - initialized as None for later definition
role_permission = rolegrant = membership = None


def define_tables(Base: DeclarativeBase, User: UserMixin, UserGroup: UserGroupMixin,
                  Role: RoleMixin, Permission: PermissionMixin) -> tuple[Table, Table, Table]:
    """
    Define association tables for the RBAC system.

    Creates and configures the database tables that manage relationships between
    users, groups, roles, and permissions. These include:
    - roles_permissions: Many-to-many relationship between roles and permissions
    - rolegrants: Many-to-many relationship between user groups and roles with context support
    - memberships: Many-to-many relationship between users and user groups

    Args:
        Base (DeclarativeBase): SQLAlchemy declarative base class for model definition
        User (UserMixin): User model mixin class
        UserGroup (UserGroupMixin): User group model mixin class
        Role (RoleMixin): Role model mixin class
        Permission (PermissionMixin): Permission model mixin class

    Returns:
        tuple[Table, Table, Table]: Three association tables in order:
            - roles_permissions (role to permission relationships)
            - rolegrants (user group to role relationships with context)
            - memberships (user to group relationships)

    Example:
        >>> from sqlalchemy.ext.declarative import declarative_base
        >>> Base = declarative_base()
        >>> define_tables(Base, UserMixin, UserGroupMixin, RoleMixin, PermissionMixin)
    """
    global role_permission, rolegrant, membership

    # Association tables
    role_permission = Table(
        'roles_permissions',
        Base.metadata,
        Column('role_id', Integer, ForeignKey(f'{Role.__tablename__}.id')),
        Column('permission_id', Integer, ForeignKey(f'{Permission.__tablename__}.id'))
    )

    rolegrant = Table(
        'rolegrants',
        Base.metadata,
        Column('usergroup_id', Integer, ForeignKey(f'{UserGroup.__tablename__}.id')),
        Column('role_id', Integer, ForeignKey(f'{Role.__tablename__}.id')),
        Column('context_id', Integer),  # This would typically be a foreign key to the context table
        Column('context_table', String),
    )

    membership = Table(
        'memberships',
        Base.metadata,
        Column('usergroup_id', Integer, ForeignKey(f'{UserGroup.__tablename__}.id')),
        Column('user_id', Integer, ForeignKey(f'{User.__tablename__}.id'))
    )

    # Establish relationships for UserGroup
    UserGroup.members = relationship(User, secondary=membership, backref='memberships')
    UserGroup.granted = relationship(Role, secondary=rolegrant, backref='grants')

    # Add owner relationship to UserGroup
    UserGroup.owner_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey(f"{User.__tablename__}.id"))
    UserGroup.owner = relationship(User, backref='self_group')

    # Establish relationships for Role
    Role.permissions = relationship(Permission, secondary=role_permission, backref='roles')

    return role_permission, rolegrant, membership
