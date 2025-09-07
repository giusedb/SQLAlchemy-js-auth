import pytest
from sqlalchemy import select, String
from sqlalchemy.orm import Mapped, mapped_column

from jsalchemy_auth import Auth
from jsalchemy_auth.models import define_tables
from jsalchemy_web_context import db


def test_schema_inversion(Person):
    from jsalchemy_auth.utils import inverted_properties
    from jsalchemy_auth.traversors import setup
    setup(Person)

    schema = {
        'country': ['departments'],
        'department': ['cities'],
        'job': ['people'],
        'hobby': ['people'],
        'city': ['people']
    }
    inv_schema = inverted_properties(schema)

    assert inv_schema == {
        'person': {'job', 'hobby', 'city'},
        'city': {'department'},
        'department': {'country'},
    }

def test_schema_inversion_2(Person):
    from jsalchemy_auth.utils import inverted_properties
    from jsalchemy_auth.traversors import setup
    setup(Person)

    schema = {
        'country': {'departments'},
        'department': {'cities'},
        'job': {'people'},
        'hobby': {'people'},
        'city': {'people'},
    }
    inv_schema = inverted_properties(schema)

    assert inv_schema == {
        'person': {'job', 'hobby', 'city'},
        'city': {'department'},
        'department': {'country'},
    }

    assert schema == inverted_properties(inv_schema)

# @pytest.mark.skip(reason="Disable due to the caching")
@pytest.mark.asyncio
async def test_actions(context, spatial, db_engine, User, Base):
    from jsalchemy_auth.traversors import setup
    from jsalchemy_auth.checkers import PathPermission

    Country, Department, City = spatial

    auth = Auth(
        base_class=Base,
        user_model=User,
        actions={
            'country': {
                'read': PathPermission('read'),
            },
            'department': {
                'read': PathPermission('read', 'country'),
            },
            'city': {
                'read': PathPermission('read', 'department.country'),
            }
        },
    )
    setup(auth.user_model)

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = auth.user_model(name='alice')
        bob = auth.user_model(name='bob')
        italy = (await db.execute(select(Country).where(Country.name == 'Italy'))).scalar()
        sicily = (await db.execute(select(Department).where(Department.name == 'Sicily'))).scalar()
        catania = (await db.execute(select(City).where(City.name == 'Catania'))).scalar()

        db.add_all([alice, bob])
        await db.commit()

        await auth.assign('reader', 'read')
        await auth.grant(alice, 'reader', italy)
        await auth.grant(bob, 'reader', sicily)

        can_alice_read_italy = await auth.can(alice, 'read', italy)
        assert can_alice_read_italy

        can_alice_read_france = await auth.can(alice, 'read', await db.get(Country, 2))
        assert not can_alice_read_france

        can_alice_read_sicily = await auth.can(alice, 'read', sicily)
        assert can_alice_read_sicily

        can_alice_read_catania = await auth.can(alice, 'read', catania)
        assert can_alice_read_catania

        all_countries = (await db.execute(select(Country))).scalars().all()
        all_departments = (await db.execute(select(Department))).scalars().all()
        all_cities = (await db.execute(select(City))).scalars().all()

        alices_countries = {country.name for country in all_countries if await auth.can(alice, 'read', country)}
        alices_departments = {department.name for department in all_departments if await auth.can(alice, 'read', department)}
        alices_cities = {city.name for city in all_cities if await auth.can(alice, 'read', city)}

        bobs_countries = {country.name for country in all_countries if await auth.can(bob, 'read', country)}
        bobs_departments = {department.name for department in all_departments if await auth.can(bob, 'read', department)}
        bobs_cities = {city.name for city in all_cities if await auth.can(bob, 'read', city)}

        assert alices_countries == {'Italy'}
        assert alices_departments == {'Sicily', 'Lombardy'}
        assert alices_cities == {'Catania', 'Milan', 'Bergamo', 'Palermo'}

        assert bobs_countries == set()
        assert bobs_departments == {'Sicily'}
        assert bobs_cities == {'Catania', 'Palermo'}

@pytest.mark.asyncio
async def test_actions_2(context, spatial, db_engine, User, Base):
    from jsalchemy_auth.traversors import setup
    from jsalchemy_auth.checkers import PathPermission

    Country, Department, City = spatial

    auth = Auth(
        base_class=Base,
        user_model=User,
        actions={
            'country': {
                'read': PathPermission('read'),
            },
            'department': {
                'read': PathPermission('read', 'country'),
            },
            'city': {
                'read': PathPermission('read', 'department'),
            }
        },
    )
    setup(auth.user_model)

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = auth.user_model(name='alice')
        bob = auth.user_model(name='bob')
        italy = (await db.execute(select(Country).where(Country.name == 'Italy'))).scalar()
        sicily = (await db.execute(select(Department).where(Department.name == 'Sicily'))).scalar()
        catania = (await db.execute(select(City).where(City.name == 'Catania'))).scalar()

        db.add_all([alice, bob])
        await db.commit()

        await auth.assign('reader', 'read')
        await auth.grant(alice, 'reader', italy)
        await auth.grant(bob, 'reader', sicily)

        can_alice_read_italy = await auth.can(alice, 'read', italy)
        assert can_alice_read_italy

        can_alice_read_france = await auth.can(alice, 'read', await db.get(Country, 2))
        assert not can_alice_read_france

        can_alice_read_sicily = await auth.can(alice, 'read', sicily)
        assert can_alice_read_sicily

        can_alice_read_catania = await auth.can(alice, 'read', catania)
        assert can_alice_read_catania == False

        all_countries = (await db.execute(select(Country))).scalars().all()
        all_departments = (await db.execute(select(Department))).scalars().all()
        all_cities = (await db.execute(select(City))).scalars().all()

        alices_countries = {country.name for country in all_countries if await auth.can(alice, 'read', country)}
        alices_departments = {department.name for department in all_departments if await auth.can(alice, 'read', department)}
        alices_cities = {city.name for city in all_cities if await auth.can(alice, 'read', city)}

        bobs_countries = {country.name for country in all_countries if await auth.can(bob, 'read', country)}
        bobs_departments = {department.name for department in all_departments if await auth.can(bob, 'read', department)}
        bobs_cities = {city.name for city in all_cities if await auth.can(bob, 'read', city)}

        assert alices_countries == {'Italy'}
        assert alices_departments == {'Sicily', 'Lombardy'}
        assert alices_cities == set()

        assert bobs_countries == set()
        assert bobs_departments == {'Sicily'}
        assert bobs_cities == {'Catania', 'Palermo'}







