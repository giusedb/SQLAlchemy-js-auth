import pytest
from jsalchemy_web_context import db
from sqlalchemy import select


@pytest.mark.asyncio
async def test_upper_traverse(context, spatial):
    Country, Department, City = spatial

    from jsalchemy_auth.traversors import traverse
    async with context() as ctx:
        city = await db.scalar(select(City).where(City.name == 'Milan'))
        cities = {item async for item in traverse(city, 'department.country.name')}

        assert 'Italy' in cities


@pytest.mark.asyncio
async def test_lower_traverse(context, spatial):
    Country, Department, City = spatial

    from jsalchemy_auth.traversors import traverse
    async with context():
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        cities = {item async for item in traverse(france, 'departments.cities.name')}

        assert 'Paris' in cities
        assert 'Lyon' in cities


@pytest.mark.asyncio
async def test_referent(context, spatial):
    from jsalchemy_auth.traversors import _referent, setup
    from jsalchemy_auth.auth import Context

    Country, Department, City = spatial
    async with context():
        italy = (await db.execute(select(Country).where(Country.name == 'Italy'))).scalar()
        setup(italy)
        for dep in (await _referent(italy, 'departments'))[1]:
            assert type(dep) is Context
            assert dep.table == 'department'

            for city in (await _referent(dep, 'cities'))[1]:
                assert type(city) is Context
                assert city.table == 'city'

                c = (await _referent(city, 'department'))[1]
                assert type(c) is Context
                assert c.table == 'department'

                c = (await _referent(city, 'department'))[1]
                assert type(c) is Context
                assert c.table == 'department'

            c = (await _referent(dep, 'country'))[1]
            assert type(c) is Context
            assert c.table == 'country'
            assert c.id == italy.id

            assert (await _referent(c, 'name'))[1] == 'Italy'

        c = (await _referent(Context('city', 10000), 'department'))[1]
        assert c is None

@pytest.mark.asyncio
async def test_lower_traverse_start(context, spatial):
    from jsalchemy_auth.traversors import traverse, to_object
    from jsalchemy_auth.auth import Context

    Country, Department, City = spatial

    async with context() as ctx:
        from jsalchemy_auth.traversors import setup
        setup(Country)
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        cities = {item async for item in traverse(france, 'departments.cities.name', start=3)}
        assert {'Paris', 'Lyon', 'Annecy', 'Calais'} == cities

        item_types = {type(item) async for item in traverse(france, 'departments.cities.name', start=2)}
        assert {Context, str} == item_types

        item_types = {type(await to_object(item)) async for item in traverse(france, 'departments.cities')}
        assert {Department, City} == item_types
        #
        cities = [Context('city', x) for x in range(1, 10)]
        countries = {c for city in cities async for c in traverse(city, 'department.country.name', 3) }
        assert {'France', 'Germany', 'Italy'} == countries

def test_treefy_paths():
    from jsalchemy_auth.traversors import treefy_paths

    result = treefy_paths('a.b.c', 'a.b.d', 'a.b.e')
    assert result == {'a.b': {'c': None, 'd': None, 'e': None}}, 'simple'

    result = treefy_paths('a.b.c', 'a.b.d', 'a.b.c.g', 'a.b.f')
    assert result == {'a.b': {'d': None, 'f': None, 'c.g': None}}, 'different length'

@pytest.mark.asyncio
async def test_resolve_attribute(context, spatial):
    """Test that it can resolve any attribute on the database from any context"""
    from jsalchemy_auth.utils import Context
    from jsalchemy_auth.traversors import resolve_attribute, setup

    Country, Department, City = spatial
    setup(Country)

    italy = Context('country', 1)
    async with context():
        assert await resolve_attribute(italy, 'name') == 'Italy'
        assert await resolve_attribute(italy, 'departments') == (Context('department', 1), Context('department', 2))
