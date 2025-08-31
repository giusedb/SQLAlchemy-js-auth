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


@pytest.mark.asyncio
async def test_actions(context, spatial, db_engine, User, Base):
    from jsalchemy_auth.traversors import PathPermission
    from jsalchemy_auth.models import define_tables

    Country, Department, City = spatial

    auth = Auth(
        base_class=Base,
        user_model=User,
        actions={
            'country': {
                'read': PathPermission('country.read'),
            },
            'department': {
                'read': PathPermission('department.read', 'country'),
            },
            'city': {
                'read': PathPermission('city.read', 'department.country'),
            }
        },
    )

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = auth.user_model(name='alice')
        await auth.assign('reader', 'read')
        await auth.grant(alice, 'reader', await db.get(Country, 1))

        italy = (await db.execute(select(Country).where(Country.name == 'Italy'))).scalar()
        sicily = (await db.execute(select(Department).where(Department.name == 'Sicily'))).scalar()
        catania = (await db.execute(select(City).where(City.name == 'Catania'))).scalar()

        can_alice_read_italy = await auth.can(alice, 'read', italy)
        assert can_alice_read_italy

        can_alice_read_france = await auth.can(alice, 'read', await db.get(Country, 2))
        assert not can_alice_read_france

        can_alice_read_sicily = await auth.can(alice, 'read', sicily)
        assert can_alice_read_sicily

        can_alice_read_catania = await auth.can(alice, 'read', catania)
        assert can_alice_read_catania






