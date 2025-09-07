# pylint: disable=redefined-outer-name
# pylint: disable=too-few-public-methods
# pylint: disable=import-outside-toplevel
import os

import pytest_asyncio
from pytest import fixture
from sqlalchemy import create_engine, Column, Integer, ForeignKey, String, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import Session, relationship, MappedColumn, DeclarativeBase, mapped_column, Mapped

from jsalchemy_auth.models import UserMixin
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
    if os.path.exists('db.sqlite'):
        os.remove('db.sqlite')
    engine = create_async_engine('sqlite+aiosqlite:///db.sqlite')
    # engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    return engine

@fixture()
def session(db_engine):
    """Create a SQLAlchemy database session."""
    return async_sessionmaker(bind=db_engine, expire_on_commit=False, autoflush=True)

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


@fixture
def User(Base):

    class User(UserMixin, Base):
        __tablename__ = "user"
        name: Mapped[str] = mapped_column(String(150), unique=True)

    return User

@pytest_asyncio.fixture
async def auth(db_engine, session, Base):
    """Create an `Auth` instance and builds the database."""
    from jsalchemy_auth import Auth
    from jsalchemy_auth import traversors
    traversors.TABLE_CLASS = None

    class User(UserMixin, Base):
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
        name:              MappedColumn[str]

    class Hobby(Base):
        __tablename__ = "hobby"
        name: MappedColumn[str]

    await define_tables(Base, db_engine)
    return Job, Hobby

@pytest_asyncio.fixture
async def Person(geo, human, Base, db_engine):
    """Create the `Person` class ."""
    class Person(Base):
        __tablename__ = "person"
        name: MappedColumn[str]
        job_id: Mapped[int] = Column(Integer, ForeignKey("job.id"))
        hobby_id: Mapped[int] = Column(Integer, ForeignKey("hobby.id"))
        city_id: Mapped[int] = Column(Integer, ForeignKey("city.id"))
        job: Mapped["Job"] = relationship("Job", backref="people")
        hobby: Mapped["Hobby"] = relationship("Hobby", backref="people")
        city: Mapped["City"] = relationship("City", backref="people")

    await define_tables(Base, db_engine)
    return Person

@pytest_asyncio.fixture
async def spatial(geo, open_session):
    """Add some data to the geo tables."""

    Country, Department, City = geo

    italy = Country(name="Italy", id=1)
    germany = Country(name="Germany", id=2)
    france = Country(name="France", id=3)

    aura = Department(name="Auvergne-Rhône-Alpes", country=france, id=1)
    ile_de_france = Department(name="Île-de-France", country=france, id=2)
    bavaria = Department(name="Bavaria", country=germany, id=3)
    brandenburg = Department(name="Brandenburg", country=germany, id=4)
    lombardy = Department(name="Lombardy", country=italy, id=5)
    sicily = Department(name="Sicily", country=italy, id=6)
    milan = City(name="Milan", department=lombardy, id=1)
    bergamo = City(name="Bergamo", department=lombardy, id=2)
    palermo = City(name="Palermo", department=sicily, id=3)
    catania = City(name="Catania", department=sicily, id=4)

    paris = City(name="Paris", department=ile_de_france, id=5)
    essonne = City(name="Essonne", department=ile_de_france, id=6)
    annecy = City(name="Annecy", department=aura, id=7)
    lyon = City(name="Lyon", department=aura, id=8)

    munich = City(name="Munich", department=bavaria, id=9)
    berlin = City(name="Potsdam", department=brandenburg, id=10)
    oranienburg = City(name="Oranienburg", department=brandenburg, id=11)

    open_session.add_all([italy, germany, france])
    open_session.add_all([aura, ile_de_france])
    open_session.add_all([bavaria, brandenburg])
    open_session.add_all([lombardy, sicily])
    open_session.add_all([milan, bergamo, palermo, catania])
    open_session.add_all([paris, essonne, annecy, lyon])
    open_session.add_all([munich, berlin, oranienburg])

    await open_session.commit()
    return geo

@pytest_asyncio.fixture
async def jobs(human, context):
    """Define a few jobjs."""
    Job, Hobby = human

    async with context():
        db.add_all([
            Job(name="Engineer"),
            Job(name="Architect"),
            Job(name="Designer"),
            Job(name="Programmer"),
            Job(name="Sales"),
        ])

@pytest_asyncio.fixture
async def hobbies(human, context):
    """Define a few hobbies."""
    Job, Hobby = human

    async with context():
        db.add_all([
            Hobby(name="Soccer"),
            Hobby(name="Football"),
            Hobby(name="Basketball"),
            Hobby(name="Baseball"),
            Hobby(name="Tennis"),
        ])

@pytest_asyncio.fixture
async def people(Person, context):
    """Define a few people."""
    async with context():
        db.add_all([
            Person(name="John"),
            Person(name="Jane"),
            Person(name="Joe"),
            Person(name="Jill"),
        ])

@pytest_asyncio.fixture
async def full_people(Person, jobs, hobbies, geo, context, people, human):
    """Define a few people with jobs, hobbies and cities."""

    Job, Hobby = human
    Country, Department, City = geo

    async with context():
        john, jane, joe, jill = [
            (await db.execute(select(Person).where(Person.name == name))).scalar()
            for name in ["John", "Jane", "Joe", "Jill"]
        ]

        engineer, architect, designer, programmer, sales = [
            (await db.execute(select(Job).where(Job.name == name))).scalar()
            for name in ["Engineer", "Architect", "Designer", "Programmer", "Sales"]
        ]

        tennis, soccer, basketball, baseball, football = [
            (await db.execute(select(Hobby).where(Hobby.name == name))).scalar()
            for name in ["Tennis", "Soccer", "Basketball", "Baseball", "Football"]
        ]

        milan, bergamo, palermo, catania = [
            (await db.execute(select(City).where(City.name == name))).scalar()
            for name in ["Milano", "Bergamo", "Palermo", "Catania"]
        ]

        john.job = engineer
        jane.job = sales
        joe.job = designer
        jill.job = programmer

        john.hobby = tennis
        jane.hobby = tennis
        joe.hobby = football
        jill.hobby = basketball

        john.city = milan
        jill.city = milan
        jane.city = palermo
        john.city = catania

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
