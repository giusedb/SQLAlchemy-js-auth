from typing import List, Optional
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, declared_attr, relationship
from sqlalchemy import String, Boolean, Integer, ForeignKey, Table, Column
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine


# Define the base mixin classes
class UserMixin:
    """Mixin for user model."""

class UserGroupMixin:
    """Mixin for user group model."""
    name: Mapped[str] = mapped_column(String(150), unique=True)
    is_personal: Mapped[bool] = mapped_column(Boolean, default=False)

class RoleMixin:
    """Mixin for role model."""
    name: Mapped[str] = mapped_column(String(150), unique=True)
    is_global: Mapped[bool] = mapped_column(Boolean, default=False)
    tables: Mapped[Optional[str]] = mapped_column(String(255))  # Comma-separated list of tables


class PermissionMixin:
    """Mixin for permission model."""
    name: Mapped[str] = mapped_column(String(150), unique=True)
    is_global: Mapped[bool] = mapped_column(Boolean, default=False)


role_permission = rolegrant = membership = None


def define_tables(Base, User: UserMixin, UserGroup: UserGroupMixin, Role: RoleMixin, Permission: PermissionMixin):
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
        Column('context_id', Integer), # This would typically be a foreign key to the context table
        Column('context_table', String),
    )

    membership = Table(
        'memberships',
        Base.metadata,
        Column('usergroup_id', Integer, ForeignKey(f'{UserGroup.__tablename__}.id')),
        Column('user_id', Integer, ForeignKey(f'{User.__tablename__}.id'))
    )

    UserGroup.members = relationship(User, secondary=membership, backref='memberships')
    UserGroup.granted = relationship(Role, secondary=rolegrant, backref='grants')
    UserGroup.owner_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey(f"{User.__tablename__}.id"))
    UserGroup.owner = relationship(User, backref='self_group')
    Role.permissions = relationship(Permission, secondary=role_permission, backref='roles')

    return role_permission, rolegrant, membership
