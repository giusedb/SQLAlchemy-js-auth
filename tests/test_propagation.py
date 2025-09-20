import pytest
from sqlalchemy import select, String

from jsalchemy_auth import Auth
from jsalchemy_web_context import db


def test_schema_inversion(Person):
    from jsalchemy_auth.utils import inverted_properties

    schema = {
        'Country': ['departments'],
        'Department': ['cities'],
        'Job': ['people'],
        'Hobby': ['people'],
        'City': ['people']
    }
    inv_schema = inverted_properties(schema, Person.registry)

    assert inv_schema == {
        'Person': {'job', 'hobby', 'city'},
        'City': {'department'},
        'Department': {'country'},
    }

def test_schema_inversion_2(Person):
    from jsalchemy_auth.utils import inverted_properties

    schema = {
        'Country': {'departments'},
        'Department': {'cities'},
        'Job': {'people'},
        'Hobby': {'people'},
        'City': {'people'},
    }
    inv_schema = inverted_properties(schema, Person.registry)

    assert inv_schema == {
        'Person': {'job', 'hobby', 'city'},
        'City': {'department'},
        'Department': {'country'},
    }

    assert schema == inverted_properties(inv_schema, Person.registry)

def test_explode_partial_schema(Person, Base):

    schema = {
        'Country': {'departments'},
        'Department': {'cities'},
        'Job': {'people'},
        'Hobby': {'people'},
        'City': {'people'},
    }
    auth = Auth(Base, propagation_schema=schema)

    inv_paths = auth._explode_partial_schema('Person')
    assert inv_paths == {'job', 'hobby', 'city', 'city.department', 'city.department.country'}

    assert auth._explode_partial_schema('City') == {
        'department', 'department.country'}

    assert auth._explode_partial_schema('Department') == {
        'country'}

    assert auth._explode_partial_schema('Country') == set()

# @pytest.mark.skip(reason="Disable due to the caching")
@pytest.mark.asyncio
async def test_actions(context, spatial, db_engine, User, Base):
    from jsalchemy_auth.checkers import Path

    Country, Department, City = spatial

    auth = Auth(
        base_class=Base,
        user_model=User,
        actions={
            'Country': {
                'read': Path('read'),
            },
            'Department': {
                'read': Path('read', 'country'),
            },
            'City': {
                'read': Path('read', 'department.country'),
            }
        },
    )
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
    from jsalchemy_auth.checkers import Path

    Country, Department, City = spatial

    auth = Auth(
        base_class=Base,
        user_model=User,
        actions={
            'Country': {
                'read': Path('read'),
            },
            'Department': {
                'read': Path('read', 'country'),
            },
            'City': {
                'read': Path('read', 'department'),
            }
        },
    )

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

@pytest.mark.asyncio
async def test_propagation(context, spatial, db_engine, User, Base, Person, full_people, human):
    Country, Department, City = spatial
    Job, Hobby = human

    auth = Auth(
        base_class=Base,
        user_model=User,
        propagation_schema={
            'Country': ['departments'],
            'Department': ['cities'],
            'Job': ['people'],
            'City': ['people'],
            'Hobby': ['people'],
        },
    )
    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = auth.user_model(name='alice')
        bob = auth.user_model(name='bob')
        italy = await Country.get_by_name('Italy')
        sicily = await Department.get_by_name('Sicily')
        catania = await City.get_by_name('Catania')

        db.add_all([alice, bob])
        await db.commit()

        await auth.assign('reader', 'read')
        await auth.grant(alice, 'reader', italy)
        await auth.grant(bob, 'reader', sicily)
        await auth.grant(alice, 'reader', await db.get(Hobby, 3))
        await auth.set_permission_global(False, 'read')

        can_alice_read_italy = await auth.can(alice, 'read', italy)
        assert can_alice_read_italy

        can_alice_read_france = await auth.can(alice, 'read', await db.get(Country, 3))
        assert not can_alice_read_france

        can_alice_read_sicily = await auth.can(alice, 'read', sicily)
        assert can_alice_read_sicily

        can_alice_read_catania = await auth.can(alice, 'read', catania)
        assert can_alice_read_catania == True

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
        assert alices_cities == {'Catania', 'Palermo', 'Bergamo', 'Milan'}

        assert bobs_countries == set()
        assert bobs_departments == {'Sicily'}
        assert bobs_cities == {'Catania', 'Palermo'}

        all_people = (await db.execute(select(Person))).scalars().all()
        alices_people = {person.name for person in all_people if await auth.can(alice, 'read', person)}
        bobs_people = {person.name for person in all_people if await auth.can(bob, 'read', person)}

        assert alices_people == {'John', 'Jane', 'Jill', 'Joe'}
        assert bobs_people == {'John', 'Jane'}

        football = (await db.execute(select(Hobby).where(Hobby.name == 'Football'))).scalar()
        await auth.grant(bob, 'reader', football)
        bobs_people = {person.name for person in all_people if await auth.can(bob, 'read', person)}
        assert bobs_people == {'John', 'Jane', 'Joe'}


