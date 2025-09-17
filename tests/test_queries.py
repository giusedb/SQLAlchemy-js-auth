import pytest
from sqlalchemy import select, String, Integer
from sqlalchemy.orm import mapped_column, Mapped, DeclarativeBase, RelationshipProperty, ColumnProperty

from jsalchemy_auth import Auth
from jsalchemy_auth.checkers import PathPermission, OwnerPermission, GroupOwnerPermission
from jsalchemy_auth.models import UserMixin
from jsalchemy_web_context import db


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

@pytest.mark.skip(reason="Review once useing the class and attributes")
@pytest.mark.asyncio
async def test_accessible_query(full_people, Person, spatial, context, db_engine, User, Base):
    Country, Department, City = spatial

    auth = Auth(Base, user_model=User,
                actions={
                    'person': {
                        'read': PathPermission('read', 'city'),
                        'write': OwnerPermission(on='city.mayor_id'),
                        'manage': GroupOwnerPermission(on='city.mayor_id')
                    }
                })

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        db.add(User(name='alice', last_name='--'))
        db.add(User(name='bob', last_name='--'))

    async with context():
        query = select(Person)
        alice = await User.get_by_name('alice')
        bob = await User.get_by_name('bob')

        await auth.assign('reader', 'read')
        await auth.assign('writer', 'read', 'write')
        await auth.assign('manager', 'read', 'write', 'manage')

        palermo = await City.get_by_name('Palermo')
        await auth.grant(alice, 'reader', palermo)

        accessible_people = (
            await db.execute(
                await auth.accessible_query(alice, query))).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == {'John', 'Jane', 'Jill', 'Joe'}