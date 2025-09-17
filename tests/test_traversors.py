from functools import reduce
from itertools import chain

import pytest

from jsalchemy_auth.traversers import traverse, flatten, setup_traversers
from jsalchemy_auth.utils import ContextSet, to_context
from jsalchemy_web_context import db
from sqlalchemy import select

@pytest.mark.asyncio
async def test_upper_traverse(context, spatial):
    Country, Department, City = spatial

    from jsalchemy_auth.traversers import traverse
    async with context() as ctx:
        city = await db.scalar(select(City).where(City.name == 'Milan'))
        countries = {item async for item in traverse(city, 'department.country.name')}

        assert ('Italy',) in countries
        assert ContextSet('country', (1,)) in countries
        assert ContextSet('department', (5,)) in countries
        assert ContextSet('department', (1,)) not in countries


@pytest.mark.asyncio
async def test_cached_trabersor(context, spatial):
    Country, Department, City = spatial

    async with context():
        city = await db.scalar(select(City).where(City.name == 'Milan'))
        countries = {item async for item in traverse(city, 'department.country.name')}

        countries_from_cache = {item async for item in traverse(city, 'department.country.name')}
        assert countries == countries_from_cache


@pytest.mark.asyncio
async def test_lower_traverse(context, spatial):
    Country, Department, City = spatial

    from jsalchemy_auth.traversers import traverse
    async with context():
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        cts = [item async for item in traverse(france, 'departments.cities.name', start=3)]
        cities = reduce(set.union, cts, set())

        assert 'Paris' in cities
        assert 'Lyon' in cities

        cts = []

        async for item in traverse(france, 'departments.cities.name', start=2):
            cts.append(item)
        cities = reduce(set.union, cts, set())
        assert {'Essonne', 'Paris', 'Annecy', 'Lyon'}.issubset(cities)
        assert len(cities) > 4


@pytest.mark.asyncio
async def test_up_and_down(context, spatial):
    Country, Department, City = spatial

    async with context():
        city = await db.scalar(select(City).where(City.name == 'Milan'))

        same_department = {item async for item in traverse(city, 'department.cities.name')}
        cities = reduce(set.union, (set(x) for x in same_department if type(x) == tuple), set())
        assert 'Milan' in cities
        assert 'Bergamo' in cities

        same_department = {item async for item in traverse(city, 'department.cities.name', start=2)}
        cities = reduce(set.union, (set(x) for x in same_department if type(x) == tuple), set())
        assert {'Milan', 'Bergamo'} == cities


@pytest.mark.asyncio
async def test_referent(context, spatial):
    from jsalchemy_auth.traversers import _referent
    from jsalchemy_auth.auth import Context

    Country, Department, City = spatial
    async with context():
        italy = (await db.execute(select(Country).where(Country.name == 'Italy'))).scalar()
        setup_traversers(italy)
        for dep in (await _referent(italy, 'departments'))[1]:
            assert type(dep) is Context
            assert dep.table == 'department'

            for city in (await _referent(dep, 'cities'))[1]:
                assert type(city) is Context
                assert city.table == 'city'

                c = (await _referent(city, 'department'))[1]
                assert isinstance(c, (Context, ContextSet))
                assert c.table == 'department'

                c = (await _referent(city, 'department'))[1]
                assert isinstance(c, (Context, ContextSet))
                assert c.table == 'department'

            c = (await _referent(dep, 'country'))[1]
            assert isinstance(c, (Context, ContextSet))
            assert c.table == 'country'
            assert italy.id in c.ids

            assert 'Italy' in (await _referent(c, 'name'))[1]

        c = (await _referent(Context(City, 10000), 'department'))[1]
        assert c is None

        c = (await _referent(ContextSet(City, (1, 3)), 'department'))[1]
        assert c.table == 'department'
        assert set(c.ids) == {5, 6}

@pytest.mark.asyncio
async def test_lower_traverse_start(context, spatial):
    from jsalchemy_auth.traversers import traverse, to_object
    from jsalchemy_auth.auth import Context

    Country, Department, City = spatial

    async with context() as ctx:
        setup_traversers(Country)
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        cities = reduce(set.union,
                        [set(item) async for item in traverse(france, 'departments.cities.name', start=3)],
                        set())
        assert {'Paris', 'Lyon', 'Annecy', 'Essonne'} == cities

        item_types = {type(item) async for item in traverse(france, 'departments.cities.name')}
        assert {ContextSet, tuple} == item_types

        item_types = {type(await to_object(i)) async for items in traverse(france, 'departments.cities') for i in items}
        assert {Department, City} == item_types
        #
        cities = [Context(City, x) for x in range(1, 10)]
        countries = {c for city in cities async for c in traverse(city, 'department.country.name', 3) }
        assert {'France', 'Germany', 'Italy'} == set(chain.from_iterable(countries))

def test_treefy_paths():
    from jsalchemy_auth.traversers import treefy_paths

    result = treefy_paths('a.b.c', 'a.b.d', 'a.b.e')
    assert result == {'a.b': {'c': None, 'd': None, 'e': None}}, 'simple'

    result = treefy_paths('a.b.c', 'a.b.d', 'a.b.c.g', 'a.b.f')
    assert result == {'a.b': {'d': None, 'f': None, 'c.g': None}}, 'different length'

@pytest.mark.asyncio
async def test_tree_traverse(context, spatial, full_people, Person):
    from jsalchemy_auth.traversers import tree_traverse
    from jsalchemy_auth.traversers import treefy_paths
    from jsalchemy_auth.traversers import setup_traversers

    Country, Department, City = spatial

    setup_traversers(Country)

    async with context():
        italy = await db.scalar(select(Country).where(Country.name == 'Italy'))
        cities = {x async for x in flatten(
            (x async for x in tree_traverse(italy, treefy_paths('departments.cities.name'), start=3)))}
        assert cities == {'Catania', 'Milan', 'Palermo', 'Bergamo'}

        activities = [x async for x in flatten(tree_traverse(
            italy,
            treefy_paths('departments.cities.people.job.name', 'departments.cities.people.hobby.name'), start=5))]
        assert set(activities) == {'Tennis', 'Engineer', 'Sales', 'Football', 'Basketball', 'Programmer', 'Designer'}

@pytest.mark.asyncio
async def test_resolve_attribute(context, spatial):
    """Test that it can resolve any attribute on the database from any context"""
    from jsalchemy_auth.traversers import resolve_attribute

    Country, Department, City = spatial
    setup_traversers(Country)

    async with context():
        italy = to_context(await db.scalar(select(Country).where(Country.name == 'Italy')))
        france = to_context(await db.scalar(select(Country).where(Country.name == 'France')))

        departments = (await resolve_attribute(france, 'departments'))[france.id]
        assert departments.table == 'department'
        assert set(departments.ids) == {1, 2}

        departments = (await resolve_attribute(italy, 'departments'))[italy.id]
        assert departments.table == 'department'
        assert set(departments.ids) == {5, 6}

        countries = await resolve_attribute(ContextSet(Department, ids=(1, 2, 3, 4)), 'country')
        countries = ContextSet.join(*countries.values())
        assert countries.table == 'country'
        assert set(countries.ids) == {2, 3}

        name = await resolve_attribute(departments, 'name')
        assert set(name.values()) == {'Lombardy', 'Sicily'}

        cities = await resolve_attribute(departments, 'cities')
        assert next(iter(cities.values())).table == 'city'
        assert reduce(set.union, (set(x.ids) for x in cities.values())) == {1, 2, 3, 4}

        countries = await resolve_attribute(departments, 'country')
        union = ContextSet.join(*countries.values())
        assert union.table == 'country'
        assert set(union.ids) == {1}


