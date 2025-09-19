import pytest
from sqlalchemy import select, String, Integer
from sqlalchemy.orm import mapped_column, Mapped, DeclarativeBase, RelationshipProperty, ColumnProperty

from jsalchemy_auth import Auth
from jsalchemy_auth.checkers import PathPermission, OwnerPermission, GroupOwnerPermission
from jsalchemy_auth.models import UserMixin
from jsalchemy_web_context import db
from tests.x import Hobby


def test_class_traverse(spatial, Person):
    from jsalchemy_auth.traversers import class_traverse

    Country, Department, City = spatial

    res = tuple(class_traverse(Person, 'city.department.country'))
    assert all(isinstance(prop, RelationshipProperty) for prop in res)
    assert len(res) == 3
    assert res[0].entity.entity == City
    assert res[1].entity.entity == Department
    assert res[2].entity.entity == Country

def test_class_traverse_inverse(Person, geo):
    from jsalchemy_auth.traversers import class_traverse

    Country, Department, City = geo

    res = tuple(class_traverse(Country, 'departments.cities'))
    assert all(isinstance(prop, RelationshipProperty) for prop in res)
    assert len(res) == 2
    assert res[0].entity.entity == Department
    assert res[1].entity.entity == City
    # assert res[2].entity.entity == Person

def test_class_traverse_column(Person, geo):
    from jsalchemy_auth.traversers import class_traverse

    Country, Department, City = geo

    res = tuple(class_traverse(Country, 'departments.cities.name'))
    assert set(map(type, res)) == {ColumnProperty, RelationshipProperty}
    assert len(res) == 3
    assert res[0].entity.entity == Department
    assert res[1].entity.entity == City
    assert res[2].parent.entity == City
    assert res[2].key == 'name'

@pytest.mark.asyncio
async def test_accessible_query(full_people, Person, spatial, context, db_engine, User, Base):
    Country, Department, City = spatial

    auth = Auth(Base, user_model=User,
                actions={
                    'Person': {
                        'read': PathPermission('read', 'city'),
                        'write': OwnerPermission(on='city.mayor_id'),
                        'manage': GroupOwnerPermission(on='city.mayor_id')
                    }
                })

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = User(name='alice', last_name='--')
        bob = User(name='bob', last_name='--')
        a_group = auth.group_model(name='Alice Corp', id=1000)
        b_group = auth.group_model(name='Bob Corp', id=1001)
        alice.memberships.append(a_group)
        bob.memberships.append(b_group)
        db.add(a_group)
        db.add(b_group)
        db.add(alice)
        db.add(bob)

    async with context():
        query = select(Person)
        alice = await User.get_by_name('alice')
        bob = await User.get_by_name('bob')

        await auth.assign('reader', 'read')

        palermo = await City.get_by_name('Palermo')
        milano = await City.get_by_name('Milan')
        essonne = await City.get_by_name('Essonne')
        essonne.mayor_id = 1000
        await auth.grant(alice, 'reader', milano)

        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'read', p)}

        a_query = await auth.accessible_query(alice, query)

        assert "FROM person JOIN city ON city.id = person.city_id \nWHERE city.id IN (1)" in str(a_query)

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == {'Jill', 'Joe'}


        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'write', p)}

        a_query = await auth.accessible_query(alice, query, 'write')

        assert "FROM person JOIN city ON city.id = person.city_id \nWHERE city.mayor_id = 1" in str(a_query)

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == {'Jane'}

        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'manage', p)}

        a_query = await auth.accessible_query(alice, query, 'manage')

        assert "FROM person JOIN city ON city.id = person.city_id \nWHERE city.mayor_id IN (1000, 1002)" in str(a_query)

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == set()

@pytest.mark.asyncio
async def test_accessible_query_tree(full_people, human, Person, spatial, context, db_engine, User, Base):
    Country, Department, City = spatial
    Job, Hobby = human

    auth = Auth(Base, user_model=User,
                actions={
                    'Person': {
                        'read': PathPermission('read',
                                               'city', 'city.department', 'city.department.country'),
                        'write': PathPermission('write',
                                                'city', 'city.department', 'city.department.country', 'job', 'hobby'),
                    }
                })

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = User(name='alice', last_name='--')
        bob = User(name='bob', last_name='--')
        db.add_all([alice, bob])

    async with context():
        query = select(Person)
        alice = await User.get_by_name('alice')
        bob = await User.get_by_name('bob')

        await auth.assign('reader', 'read')
        await auth.assign('editor', 'read', 'write')
        await auth.assign('manager', 'read', 'write', 'manage')

        palermo = await City.get_by_name('Palermo')
        milano = await City.get_by_name('Milan')
        essonne = await City.get_by_name('Essonne')
        essonne.mayor_id = 1000
        await auth.grant(alice, 'reader', milano)
        await auth.grant(alice, 'reader', await Hobby.get_by_name('Tennis'))
        await auth.grant(alice, 'reader', await Job.get_by_name('Programmer'))

        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'read', p)}

        a_query = await auth.accessible_query(alice, query)

        assert 'city ON city.id' in str(a_query)
        assert 'department ON department.id' not in str(a_query)
        assert 'country ON country.id' not in str(a_query)
        assert 'job ON job.id' not in str(a_query)
        assert 'hobby ON hobby.id' not in str(a_query)

        assert "city.id IN (1)" in str(a_query)


        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == {'Jill', 'Joe'}


        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'write', p)}

        a_query = await auth.accessible_query(alice, query, 'write')

        assert "where false" in str(a_query).lower()

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == set()

        await auth.grant(alice, 'manager', await Job.get_by_name('Programmer'))
        await auth.grant(alice, 'manager', essonne)
        await auth.grant(alice, 'manager', await Country.get_by_name('Germany'))
        await auth.grant(alice, 'manager', palermo)

        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'write', p)}

        a_query = await auth.accessible_query(alice, query, 'write')

        assert "JOIN city ON city.id = person.city_id" in str(a_query)
        assert "JOIN department ON department.id = city.department_id" in str(a_query)
        assert "JOIN country ON country.id = department.country_id" in str(a_query)
        assert "JOIN job ON job.id = person.job_id" in str(a_query)
        assert "JOIN hobby ON hobby.id = person.hobby_id" not in str(a_query)

        assert "job.id IN (4)" in str(a_query)
        assert "city.id IN (3, 6)" in str(a_query)
        assert "country.id IN (2)" in str(a_query)

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == {'Jane', 'Jill'}

        b_query = await auth.accessible_query(bob, query)



