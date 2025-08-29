import pytest
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import DeclarativeBase, MappedColumn, mapped_column, Mapped, relationship

from jsalchemy_auth import Auth
from jsalchemy_auth.models import UserMixin, UserGroupMixin, RoleMixin, PermissionMixin


def test_sync_create_tables(sync_db_engine):
    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "user"
        id: MappedColumn[int] = Column(Integer, primary_key=True)
        username: MappedColumn[str]

    auth = Auth(Base, user_model=User)

    Base.metadata.create_all(sync_db_engine)

    assert auth.user_model is User
    assert auth.group_model is not None
    assert auth.role_model is not None
    assert auth.permission_model is not None

    all_models = [auth.user_model, auth.group_model, auth.role_model, auth.permission_model]
    assert all(model.__tablename__ in Base.metadata.tables for model in all_models)

@pytest.mark.asyncio
async def test_async_create_tables(db_engine):
    class Base(DeclarativeBase):
        pass

    class User(Base):
        __tablename__ = "user"
        id: MappedColumn[int] = Column(Integer, primary_key=True)
        username: MappedColumn[str]

    auth = Auth(Base, user_model=User)

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    assert auth.user_model is User
    assert auth.group_model is not None
    assert auth.role_model is not None
    assert auth.permission_model is not None

    all_models = [auth.user_model, auth.group_model, auth.role_model, auth.permission_model]
    assert all(model.__tablename__ in Base.metadata.tables for model in all_models)


def test_db_relations(sync_db_engine):
    from sqlalchemy.orm import sessionmaker

    class Base(DeclarativeBase):
        pass

    class User(UserMixin, Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(primary_key=True)
        username: Mapped[str]

    auth = Auth(Base, user_model=User)

    Base.metadata.create_all(sync_db_engine)

    session = sessionmaker(sync_db_engine)()
    with session.begin():
        user = User(username="test")
        group = auth.group_model(name="test")
        role = auth.role_model(name="test", tables='cities')
        permission = auth.permission_model(name="test")

        role.permissions.append(permission)
        group.granted.append(role)
        user.memberships.append(group)

        session.add(user)
        session.add(group)
        session.add(role)
        session.add(permission)
        session.commit()

    with session.begin():
        user = session.query(User).first()
        assert user.username == "test"
        assert user.memberships[0].name == "test"
        assert user.memberships[0].granted[0].name == "test"
        assert user.memberships[0].granted[0].permissions[0].name == "test"

        permission = session.query(auth.permission_model).first()
        assert permission.name == "test"
        assert permission.roles[0].name == "test"
        assert permission.roles[0].grants[0].name == "test"
        assert permission.roles[0].grants[0].members[0].username == "test"


def test_db_relations_custom_classes(sync_db_engine):
    from sqlalchemy.orm import sessionmaker

    class Base(DeclarativeBase):
        pass

    class City(Base):
        __tablename__ = "cities"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

    class User(UserMixin, Base):
        __tablename__ = "user"
        id: Mapped[int] = mapped_column(primary_key=True)
        username: Mapped[str]

    class Group(UserGroupMixin, Base):
        __tablename__ = "group"
        id: Mapped[int] = mapped_column(primary_key=True)
        description: Mapped[str]

    class Role(RoleMixin, Base):
        __tablename__ = "role"
        id: Mapped[int] = mapped_column(primary_key=True)
        description: Mapped[str]
        city_id: Mapped[int] = mapped_column(ForeignKey("cities.id"))
        city: Mapped["City"] = relationship("City", backref="roles")

    class Permission(PermissionMixin, Base):
        __tablename__ = "permission"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

    auth = Auth(Base, user_model=User, group_model=Group, role_model=Role, permission_model=Permission)

    Base.metadata.create_all(sync_db_engine)

    session = sessionmaker(sync_db_engine)()
    with session.begin():
        city = City(name="test city")
        user = User(username="test")
        group = Group(name="test", description="test descritpion")
        role = auth.role_model(name="test", tables='cities', city=city, description="test descritpion")
        permission = auth.permission_model(name="test")

        role.permissions.append(permission)
        group.granted.append(role)
        user.memberships.append(group)

        session.add(user)
        session.add(group)
        session.add(role)
        session.add(permission)
        session.commit()

    with session.begin():
        user = session.query(User).first()
        assert user.username == "test"
        assert user.memberships[0].name == "test"
        assert user.memberships[0].granted[0].name == "test"
        assert user.memberships[0].granted[0].permissions[0].name == "test"

        permission = session.query(auth.permission_model).first()
        assert permission.name == "test"
        assert permission.roles[0].name == "test"
        assert permission.roles[0].grants[0].name == "test"
        assert permission.roles[0].grants[0].members[0].username == "test"

        assert permission.roles[0].city.name == "test city"
        assert permission.roles[0].description == "test descritpion"

