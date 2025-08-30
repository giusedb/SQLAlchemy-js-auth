# pylint: disable=redefined-outer-name
# pylint: disable=too-few-public-methods
# pylint: disable=import-outside-toplevel
import pytest_asyncio
from pytest import fixture
from sqlalchemy import create_engine, Column, Integer, ForeignKey, String, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import Session, relationship, MappedColumn, DeclarativeBase, mapped_column, Mapped
from jsalchemy_web_context import session, db, request
from fakeredis.aioredis import FakeRedis


async def define_tables(Base, db_engine):
    """Define the tables."""
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@fixture
def sync_db_engine():
    """Create a test SQLAlchemy database engine."""
    engine = create_engine('sqlite:///:memory:')
    return engine


@fixture()
def db_engine():
    """Create a test SQLAlchemy database engine."""
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    return engine

@fixture()
def session(db_engine):
    """Create a SQLAlchemy database session."""
    return async_sessionmaker(bind=db_engine, expire_on_commit=False)

@fixture
def open_session(session):
    """Open a session for a request."""
    return session()

@fixture()
def context(session):
    """Build the jsalchemy_web_context context manager."""
    from jsalchemy_web_context import ContextManager

    return ContextManager(session, FakeRedis.from_url('redis://localhost:6379/0'))

@fixture
def Base():
    """Create a SQLAlchemy declarative base class."""
    class Base(AsyncAttrs, DeclarativeBase):
        id: MappedColumn[int] = Column(Integer, primary_key=True)

        def __repr__(self):
            return f"{self.__class__.__name__}(name={self.name})"

        def __str__(self):
            return f"[{self.name}-{self.id}]"

    return Base

@pytest_asyncio.fixture
async def auth(db_engine, session, Base):
    """Create an `Auth` instance and builds the database."""
    from jsalchemy_auth import Auth

    class User(Base):
        __tablename__ = "user"
        name: Mapped[str] = mapped_column(String(150), unique=True)

    auth = Auth(Base, user_model=User)

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    return auth

@pytest_asyncio.fixture
async def user_auth(auth: "jsalchemy_auth.auth.Auth", context):
    """Create a simple user scenario."""

    async with context():
        for name in ['foo', 'bar', 'baz']:
            db.add(auth.user_model(name=name))
        for name in ['admin', 'superadmin', 'local users', 'users']:
            db.add(auth.group_model(name=name))
        for role in ['admin', 'superadmin', 'local users', 'users']:
            db.add(auth.role_model(name=role))

        await db.commit()

        users = {user.name: user for user in (await db.execute(select(auth.user_model))).scalars()}
        groups = {group.name: group for group in (await db.execute(select(auth.group_model))).scalars()}
        roles = {role.name: role for role in (await db.execute(select(auth.role_model))).scalars()}

        (await users['foo'].awaitable_attrs.memberships).append(groups['admin'])
        (await users['bar'].awaitable_attrs.memberships).append(groups['superadmin'])
        for name, group in groups.items():
            (await group.awaitable_attrs.granted).append(roles[name])

    return users, groups, roles


@pytest_asyncio.fixture
async def geo(Base, db_engine):
    """Create a simple geo scenario with `Country`, `Department` and `City`."""
    class Country(Base):
        __tablename__ = "country"
        name: MappedColumn[str]

    class Department(Base):
        __tablename__ = "department"
        name: MappedColumn[str]
        country_id: MappedColumn[int] = Column(Integer, ForeignKey("country.id"))
        country: MappedColumn["Country"] = relationship("Country", backref="departments")

    class City(Base):
        __tablename__ = "city"
        name: MappedColumn[str]
        department_id: MappedColumn[int] = Column(Integer, ForeignKey("department.id"))
        department: MappedColumn["Department"] = relationship("Department", backref="cities")

    await define_tables(Base, db_engine)
    return Country, Department, City

@pytest_asyncio.fixture
async def human(Base, db_engine):
    """Create a simple human scenario with `Job` and `Hobby`."""

    class Job(Base):
        __tablename__ = "job"
        name: MappedColumn[str]

    class Hobby(Base):
        __tablename__ = "hobby"
        name: MappedColumn[str]

    await define_tables(Base, db_engine)
    return Job, Hobby


@pytest_asyncio.fixture
async def Person(geo, human):
    """Create the `Person` class ."""
    class Person(Base):
        __tablename__ = "person"
        name: MappedColumn[str]
        job: MappedColumn["Job"] = relationship("Job", backref="people")
        hobby: MappedColumn["Hobby"] = relationship("Hobby", backref="people")
        city: MappedColumn["City"] = relationship("City", backref="people")

    await define_tables(Base, db_engine)
    return Person

@pytest_asyncio.fixture
async def spatial(geo, open_session):
    """Add some data to the geo tables."""

    Country, Department, City = geo

    italy = Country(name="Italy")
    germany = Country(name="Germany")
    france = Country(name="France")

    ain = Department(name="Ain", country=france)
    ile_de_france = Department(name="Ile de France", country=france)
    bavaria = Department(name="Bavaria", country=germany)
    north_germany = Department(name="North Germany", country=germany)
    lombardy = Department(name="Lombardy", country=italy)
    sicily = Department(name="Sicily", country=italy)
    milan = City(name="Milan", department=lombardy)
    bergamo = City(name="Bergamo", department=lombardy)
    palermo = City(name="Palermo", department=sicily)
    catania = City(name="Catania", department=sicily)

    paris = City(name="Paris", department=ile_de_france)
    calais = City(name="Calais", department=ile_de_france)
    annecy = City(name="Annecy", department=ain)
    lyon = City(name="Lyon", department=ain)

    munich = City(name="Munich", department=bavaria)
    berlin = City(name="Berlin", department=north_germany)
    bonn = City(name="Bonn", department=north_germany)

    open_session.add_all([italy, germany, france])
    open_session.add_all([ain, ile_de_france])
    open_session.add_all([bavaria, north_germany])
    open_session.add_all([lombardy, sicily])
    open_session.add_all([milan, bergamo, palermo, catania])
    open_session.add_all([paris, calais, annecy, lyon])
    open_session.add_all([munich, berlin, bonn])

    await open_session.commit()
    return geo

@fixture
def jobs(human, session):
    """Define a few jobjs."""
    Job, Hobby = human

    session.add_all([
        Job(name="Engineer"),
        Job(name="Architect"),
        Job(name="Designer"),
        Job(name="Programmer"),
        Job(name="Sales"),
    ])
    session.commit()

@fixture
def hobbies(human, session):
    """Define a few hobbies."""
    Job, Hobby = human

    session.add_all([
        Hobby(name="Soccer"),
        Hobby(name="Football"),
        Hobby(name="Basketball"),
        Hobby(name="Baseball"),
        Hobby(name="Tennis"),
    ])
    session.commit()

@fixture
def people(Person, session):
    """Define a few people."""
    session.add_all([
        Person(name="John"),
        Person(name="Jane"),
        Person(name="Joe"),
        Person(name="Jill"),
    ])
    session.commit()

@fixture
def full_people(Person, jobs, hobbies, geo, session, define_tables):
    """Define a few people with jobs, hobbies and cities."""

    john = session.query(Person).filter(Person.name == "John").one()
    jane = session.query(Person).filter(Person.name == "Jane").one()
    joe = session.query(Person).filter(Person.name == "Joe").one()
    jill = session.query(Person).filter(Person.name == "Jill").one()

    john.job = session.query(Job).filter(Job.name == "Engineer").one()
    jane.job = session.query(Job).filter(Job.name == "Sales").one()
    joe.job = session.query(Job).filter(Job.name == "Designer").one()
    jill.job = session.query(Job).filter(Job.name == "Programmer").one()

    john.hobby = session.query(Hobby).filter(Hobby.name == "Tennis").one()
    jane.hobby = session.query(Hobby).filter(Hobby.name == "Tennis").one()
    joe.hobby = session.query(Hobby).filter(Hobby.name == "Football").one()
    jill.hobby = session.query(Hobby).filter(Hobby.name == "Basketball").one()

    john.city = session.query(City).filter(City.name == "Milan").one()
    jill.city = session.query(City).filter(City.name == "Milan").one()
    jane.city = session.query(City).filter(City.name == "Palermo").one()
    john.city = session.query(City).filter(City.name == "Catania").one()

    session.commit()

@pytest_asyncio.fixture
async def roles(auth, context):
    roles = {
        'admin': ['create', 'read', 'update', 'delete'],
        'read-only': ['read'],
        'editor': ['create', 'update'],
    }

    async with context() as ctx:
        for role, permissions in roles.items():
            await auth.assign(role, *permissions)

    return roles

@pytest_asyncio.fixture
async def users(auth, roles, context):
    users = {
        'alice',
        'bob',
        'charlie',
    }

    async with context() as ctx:
        for user_name in users:
            user = auth.user_model(name=user_name)
            db.add(user)
            db.add(auth.group_model(name=user_name, members=[user]))

        await db.commit()

    return users
