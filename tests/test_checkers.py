import pytest

from jsalchemy_auth import Auth
from jsalchemy_auth.checkers import OwnerPermission
from jsalchemy_web_context import db


@pytest.mark.asyncio
async def test_owner_permission(Base, spatial, context, users, auth):
    Country, Department, City = spatial
    auth.actions={
        'city': {            'manage': OwnerPermission(on='mayor_id')},
        'department': {
            'manage': OwnerPermission(on='president_id')},
        'country': {
            'manage': OwnerPermission(on='president_id')},
    }

    async with context():
        italy, germany = [await db.get(Country, x) for x in (1, 2)]
        palermo = await City.get_by_name('Palermo')
        essonne = await City.get_by_name('Essonne')
        aura = await Department.get_by_name('Auvergne-Rhône-Alpes')
        bavaria = await Department.get_by_name('Bavaria')
        alice, bob, charlie = [await db.get(auth.user_model, x) for x in (1, 2, 3)]

        assert await auth.can(alice, 'manage', palermo)
        assert not await auth.can(bob, 'manage', palermo)
        assert not await auth.can(charlie, 'manage', palermo)

        assert await auth.can(alice, 'manage', aura)
        assert not await auth.can(bob, 'manage', aura)
        assert not await auth.can(charlie, 'manage', aura)

        assert not await auth.can(alice, 'manage', italy)
        assert await auth.can(bob, 'manage', italy)
        assert not await auth.can(charlie, 'manage', italy)


@pytest.mark.asyncio
async def test_owner_long(Base, spatial, context, users, auth):
    Country, Department, City = spatial
    auth.actions={
        'city': {
            'manage': OwnerPermission(on='department.country.president_id')},
        'department': {
            'manage': OwnerPermission(on='country.president_id')},
        'country': {
            'manage': OwnerPermission(on='president_id')},
    }

    async with context():
        italy, germany = [await db.get(Country, x) for x in (1, 2)]
        palermo = await db.get(City, 3)
        essonne = await db.get(City, 6)
        aura = await db.get(Department, 1)
        bavaria = await db.get(Department, 3)
        alice, bob, charlie = [await db.get(auth.user_model, x) for x in (1, 2, 3)]

        assert not await auth.can(alice, 'manage', palermo)
        assert await auth.can(bob, 'manage', palermo)
        assert not await auth.can(charlie, 'manage', palermo)
        assert not await auth.can(alice, 'manage', aura)
        assert not await auth.can(bob, 'manage', aura)
        assert not await auth.can(charlie, 'manage', aura)
        assert not await auth.can(alice, 'manage', italy)
        assert await auth.can(bob, 'manage', italy)
        assert not await auth.can(charlie, 'manage', essonne)


@pytest.mark.asyncio
async def test_owner_combined(Base, spatial, context, users, auth):
    Country, Department, City = spatial
    auth.actions={
        'city': {
            'manage': OwnerPermission(on='mayor_id') |
                      OwnerPermission(on='department.president_id') |
                      OwnerPermission(on='department.country.president_id')},
        'department': {
            'manage': OwnerPermission(on='country.president_id') |
                      OwnerPermission(on='president_id')},
        'country': {
            'manage': OwnerPermission(on='president_id')},
    }

    async with context():
        italy, germany = [await db.get(Country, x) for x in (1, 2)]
        palermo = await db.get(City, 3)
        munich = await db.get(City, 9)
        essonne = await db.get(City, 6)
        aura = await db.get(Department, 1)
        bavaria = await db.get(Department, 3)
        alice, bob, charlie = [await db.get(auth.user_model, x) for x in (1, 2, 3)]


        assert await auth.can(alice, 'manage', palermo)  # she is mayor
        assert await auth.can(bob, 'manage', palermo)  # he is president
        assert await auth.can(charlie, 'manage', munich)  # he is Bavarian's president
        assert await auth.can(charlie, 'manage', bavaria)  # he is Bavarian's president
        assert not await auth.can(charlie, 'manage', palermo)
        assert await auth.can(alice, 'manage', aura)
        assert not await auth.can(bob, 'manage', aura)
        assert not await auth.can(charlie, 'manage', aura)
        assert not await auth.can(alice, 'manage', italy)
        assert await auth.can(bob, 'manage', italy)
        assert not await auth.can(charlie, 'manage', essonne)
        assert not await auth.can(alice, 'manage', essonne)
        assert await auth.can(bob, 'manage', essonne)

