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
    async with context() as ctx:
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
        for dep in await _referent(italy, 'departments'):
            assert type(dep) is Context
            assert dep.table == 'department'

            for city in await _referent(dep, 'cities'):
                assert type(city) is Context
                assert city.table == 'city'

                c = await _referent(city, 'department')
                assert type(c) is Context
                assert c.table == 'department'

                c = await _referent(city, 'department')
                assert type(c) is Context
                assert c.table == 'department'

            c = await _referent(dep, 'country')
            assert type(c) is Context
            assert c.table == 'country'
            assert c.id == italy.id

            assert await _referent(c, 'name') == 'Italy'

        c = await _referent(Context('city', 10000), 'departments')
        assert c is None

@pytest.mark.asyncio
async def test_lower_traverse_start(context, spatial):
    Country, Department, City = spatial

    from jsalchemy_auth.traversors import traverse
    async with context() as ctx:
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        cities = {item async for item in traverse(france, 'departments.cities.name', start=3)}
        assert {'Paris', 'Lyon', 'Annecy', 'Calais'} == cities

        item_types = {type(item) async for item in traverse(france, 'departments.cities.name', start=2)}
        assert {City, str} == item_types

        from jsalchemy_auth.traversors import to_object
        item_types = {type(await to_object(item)) async for item in traverse(france, 'departments.cities')}
        assert {Department, City} == item_types

        from jsalchemy_auth.auth import Context
        cities = [Context('city', x) for x in range(1, 10)]
        countries = {c for city in cities async for c in traverse(await to_object(city), 'department.country.name', 3) }
        assert {'France', 'Germany', 'Italy'} == countries

