import pytest
from sqlalchemy import create_engine, Column, Integer, ForeignKey, String, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import Session, relationship, MappedColumn, DeclarativeBase, mapped_column, Mapped
from jsalchemy_web_context import session, db, request
from fakeredis.aioredis import FakeRedis
from jsalchemy_web_context import ContextManager

from jsalchemy_auth import Auth
from jsalchemy_auth.traversers import _referent, setup_traversers, traverse, to_object


async def define_tables():
    """Define the tables."""
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

db_engine = create_async_engine('sqlite+aiosqlite:///:memory:')

context = ContextManager(async_sessionmaker(bind=db_engine), FakeRedis.from_url('redis://localhost:6379/0'))

class Base(AsyncAttrs, DeclarativeBase):
    id: MappedColumn[int] = Column(Integer, primary_key=True)

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"

    def __str__(self):
        return f"[{self.name}-{self.id}]"

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

class Job(Base):
    __tablename__ = "job"
    name: MappedColumn[str]

class Hobby(Base):
    __tablename__ = "hobby"
    name: MappedColumn[str]

class Person(Base):
    __tablename__ = "person"
    name: MappedColumn[str]
    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"), nullable=True)
    job: MappedColumn["Job"] = relationship("Job", backref="people")
    hobby_id: Mapped[int] = mapped_column(ForeignKey("hobby.id"), nullable=True)
    hobby: MappedColumn["Hobby"] = relationship("Hobby", backref="people")
    city_id: Mapped[int] = mapped_column(ForeignKey("city.id"), nullable=True)
    city: MappedColumn["City"] = relationship("City", backref="people")


class User(Base):
    __tablename__ = "user"
    name: Mapped[str] = mapped_column(String(150), unique=True)


async def user_auth():
    """Create a simple user scenario."""

    async with context() as conn:
        for name in ['foo', 'bar', 'baz']:
            session.add(auth.user_model(name=name))

        for name in ['admin', 'superadmin', 'local users', 'users']:
            session.add(auth.user_group_model(name=name))

        for role in ['admin', 'superadmin']:
            session.add(auth.role_model(name=role))

        db.commit()

        users = { user.username: user for user in session.query(auth.user_model) }
        groups = { group.name: group for group in session.query(auth.user_group_model) }
        roles = { role.name: role for role in session.query(auth.role_model) }

        users['foo'].groups.append(groups['admin'])
        users['bar'].groups.append(groups['superadmin'])

        db.commit()


async def spatial():
    """Add some data to the geo tables."""

    async with context():
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

        db.add_all([italy, germany, france])
        db.add_all([ain, ile_de_france])
        db.add_all([bavaria, north_germany])
        db.add_all([lombardy, sicily])
        db.add_all([milan, bergamo, palermo, catania])
        db.add_all([paris, calais, annecy, lyon])
        db.add_all([munich, berlin, bonn])


async def jobs():
    """Define a few jobjs."""
    async with context() as ctx:
        db.add_all([
            Job(name="Engineer"),
            Job(name="Architect"),
            Job(name="Designer"),
            Job(name="Programmer"),
            Job(name="Sales"),
        ])
        db.commit()

async def hobbies():
    """Define a few hobbies."""
    async with context() as ctx:
        db.add_all([
            Hobby(name="Soccer"),
            Hobby(name="Football"),
            Hobby(name="Basketball"),
            Hobby(name="Baseball"),
            Hobby(name="Tennis"),
        ])
        db.commit()

async def people(Person, session):
    """Define a few people."""
    async with context() as ctx:
        db.add_all([
            Person(name="John"),
            Person(name="Jane"),
            Person(name="Joe"),
            Person(name="Jill"),
        ])
        db.commit()

async def full_people():
    """Define a few people with jobs, hobbies and cities."""

    async with context():
        john = (await db.execute(select(Person).where(Person.name == "John"))).scalar()
        jane = (await db.execute(select(Person).where(Person.name == "Jane"))).scalar()
        joe = (await db.execute(select(Person).where(Person.name == "Joe"))).scalar()
        jill = (await db.execute(select(Person).where(Person.name == 'Jill'))).scalar()

        john.job = (await db.execute(select(Job).where(Job.name == "Engineer"))).scalar()
        jane.job = (await db.execute(select(Job).where(Job.name == "Sales"))).scalar()
        joe.job = (await db.execute(select(Job).where(Job.name == "Architect"))).scalar()
        jill.job = (await db.execute(select(Job).where(Job.name == "Programmer"))).scalar()

        john.hobby = (await db.execute(select(Hobby).where(Hobby.name == "Soccer"))).scalar()
        jane.hobby = (await db.execute(select(Hobby).where(Hobby.name == "Tennis"))).scalar()
        joe.hobby = (await db.execute(select(Hobby).where(Hobby.name == "Football"))).scalar()
        jill.hobby = (await db.execute(select(Hobby).where(Hobby.name == "Basketball"))).scalar()

        john.city = (await db.execute(select(City).where(City.name == "Milano"))).scalar()
        jane.city = (await db.execute(select(City).where(City.name == "Milano"))).scalar()
        joe.city = (await db.execute(select(City).where(City.name == "Palermo"))).scalar()
        jill.city = (await db.execute(select(City).where(City.name == "Catania"))).scalar()




async def roles(auth):
    roles = {
        'admin': ['create', 'read', 'update', 'delete'],
        'read-only': ['read'],
        'editor': ['create', 'update'],
    }

    async with context() as ctx:
        for role, permissions in roles.items():
            await auth.assign(role, *permissions)

    return roles

async def users(auth):
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

async def prova():
    async with context():
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        cities = {item async for item in traverse(france, 'departments.cities.name')}

        assert 'Paris' in cities
        assert 'Lyon' in cities


import asyncio
auth = Auth(Base, user_model=User)
asyncio.run(define_tables())

asyncio.run(spatial())
asyncio.run(users(auth))
asyncio.run(roles(auth))


asyncio.run(jobs())
asyncio.run(hobbies())
asyncio.run(people(Person, session))
asyncio.run(full_people())



