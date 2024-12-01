# pylint: disable=redefined-outer-name
# pylint: disable=too-few-public-methods
# pylint: disable=import-outside-toplevel


from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session


@fixture()
def db_engine():
    """Create a test SQLAlchemy database engine."""
    engine = create_engine('sqlite:///:memory:')
    return engine


@fixture()
def session(db_engine):
    """Create a SQLAlchemy database session."""
    return sessionmaker(bind=db_engine)()

@fixture()
def auth(db_engine):
    """Create an `Auth` instance and builds the database."""
    from quasar_authorization.auth import Auth

    class MyBase(DeclarativeBase):
        """Base declarative class to use for testing."""

    auth = Auth(MyBase)

    MyBase.metadata.create_all(db_engine)

    return auth

@fixture()
def user_auth(session: Session, auth: "quasar_authorization.auth.Auth"):
    """Create a simple user scenario."""
    for username, password in [['foo', 'foo'], ['bar', 'bar'], ['baz', 'baz']]:
        session.add(auth.User(username=username, password=password))

    for name in ['admin', 'superadmin', 'local users', 'users']:
        session.add(auth.Group(name=name))

    for role in ['admin', 'superadmin']:
        session.add(auth.Role(name=role))

    session.commit()

    users = { user.username: user for user in session.query(auth.User) }
    groups = { group.name: group for group in session.query(auth.Group) }
    roles = { role.name: role for role in session.query(auth.Role) }

    users['foo'].membership.append(groups['admin'])
    users['bar'].membership.append(groups['superadmin'])

    session.commit()
    return users, groups, roles
