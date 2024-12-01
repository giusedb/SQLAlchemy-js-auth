# pylint: disable=too-few-public-methods
from typing import List

from sqlalchemy import String, ForeignKey, Column, Table, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.orm import relationship

class Base:
    """Base model mixin with the id primary key"""
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

class NamedMixin:
    """Add name and use it to represent the entity."""
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r})"


class UserMixin(Base):
    """Minimalistic user model class."""

    def join(self, group: "GroupMixin"):
        """Make the user to join the `group`."""
        group.members.append(self)

    def leave(self, group: "GroupMixin"):
        """Make the user to leave the `group`."""
        group.members.remove(self)


class GroupMixin(NamedMixin, Base):
    """Minimalistic Group model class."""

    def add_member(self, user: "UserMixin"):
        """Make the `user` to join this group."""
        self.members.append(user)


class RoleMixin(NamedMixin, Base):
    """Minimalistic Role model class."""



class PermissionMixin(NamedMixin, Base):
    """Minimalistic Permission model class."""

    is_global: Mapped[bool] = mapped_column(default=False, nullable=True, server_default=False)



def define_relation_tables(auth, metadata):
    """Define connection tables and relationships among the above model classes."""

    role_permission = metadata.tables.get("auth_role_permission")
    if role_permission is None:
        role_permission = Table(
            "auth_role_permission",
            metadata,
            Column("role_id", Integer, ForeignKey(f"{auth.Role.__tablename__}.id")),
            Column("permission_id", Integer, ForeignKey(f"{auth.Permission.__tablename__}.id")),
        )
        auth.Permission.roles: Mapped[List[auth.Role]] = relationship(
            auth.Role, secondary=role_permission, backref='permissions')

    assignment = metadata.tables.get("auth_role_assignment")
    if assignment is None:
        assignment = Table(
            'auth_role_assignment',
            metadata,
            Column("role_id", Integer, ForeignKey(f"{auth.Role.__tablename__}.id"), nullable=False),
            Column("group_id", Integer, ForeignKey(f"{auth.Group.__tablename__}.id"), nullable=True),
            Column("user_id", Integer, ForeignKey(f"{auth.User.__tablename__}.id"), nullable=True),
            Column("table", String, nullable=True, default='global'),
            Column("pk", String, nullable=False, default=0),
        )
        auth.Role.users: Mapped[List[auth.User]] = relationship(auth.User, secondary=assignment)
        auth.Group.users: Mapped[List[auth.User]] = relationship(auth.User, secondary=assignment)
        auth.User.roles: Mapped[List[auth.Role]] = relationship(auth.Role, secondary=assignment)
        auth.User.groups: Mapped[[List[auth.Group]]] = relationship(auth.Group, secondary=assignment)

    user_groups = metadata.tables.get("auth_user_membership")
    if user_groups is None:
        user_groups = Table(
            "auth_user_membership",
            metadata,
            Column("user_id", Integer, ForeignKey(f"{auth.User.__tablename__}.id"), nullable=False),
            Column("group_id", Integer, ForeignKey(f"{auth.Group.__tablename__}.id"), nullable=False),
        )
        auth.User.membership: Mapped[List[auth.Group]] = relationship(auth.Group, secondary=user_groups)
        auth.Group.members: Mapped[List[auth.User]] = relationship(auth.User, secondary=user_groups)
    return locals()


def create_from_mixin(mixin: Base, model_base: DeclarativeBase) -> Base:
    """Generate the model class mixins as applying the minimalist approach."""
    name = mixin.__name__.replace('Mixin', '')
    table_name = f"auth_{name.lower()}"
    if table_name not in model_base.metadata.tables:
        model = type(name, (mixin, model_base),
                     {'__tablename__': table_name, 'extend_existing': True})
        return model
    return next(filter(
        lambda m: m.class_.__name__ == name,
        model_base.registry.mappers)).class_
