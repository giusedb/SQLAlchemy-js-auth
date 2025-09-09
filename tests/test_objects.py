import asyncio

import pytest
from sqlalchemy import select
from jsalchemy_web_context import db, session, request
from sqlalchemy import literal

from jsalchemy_auth import Auth
from jsalchemy_auth.auth import PermissionGrantError


def test_context(context):
    async def run():
        async with context() as ctx:
            assert ctx.token is not None, 'token is None'
            session.pippo = 'ciao'
            assert session.pippo == 'ciao'
            request.pippo = 'ciao'
            assert request.pippo == 'ciao'
            one = (await db.execute(select(literal(1)))).scalar()
            assert one == 1

    asyncio.run(run())

@pytest.mark.asyncio
async def test_setup(spatial, context, Base):
    auth = Auth(
        base_class=Base,
        propagation_schema={
            'country': ['departments'],
            'department': ['cities'],
        },
    )


@pytest.mark.asyncio
async def test_db(spatial, context):
    Country, Department, City = spatial

    async with context() as ctx:
        countries = (await db.execute(select(Country))).scalars().all()
        assert len(countries) == 3

@pytest.mark.asyncio
async def test_grants(spatial, context, auth, roles, users):
    Country, Department, City = spatial

    async with context() as ctx:
        alice = await db.get(auth.group_model, 1)
        bob = await db.get(auth.group_model, 2)
        charlie = await db.get(auth.group_model, 3)
        await auth.grant(alice, 'admin', await db.get(Country, 1))
        await auth.grant(bob, 'read-only', await db.get(Country, 1))
        await auth.grant(charlie, 'editor', await db.get(Country, 1))

        with pytest.raises(PermissionGrantError):
            await auth.grant(alice, 'dontexists', await db.get(Country, 1))

@pytest.mark.asyncio
async def test_direct_permissions(auth, spatial, context, roles, users):
    Country, Department, City = spatial

    async with context() as ctx:
        alice = await db.get(auth.group_model, 1)
        await auth.grant(alice, 'admin', await db.get(Country, 1))

        await auth.grant(alice, 'read-only', await db.get(Country, 1))

        assert await auth.has_permission(alice, 'read', await db.get(Country, 1)) == True
