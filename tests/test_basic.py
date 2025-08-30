import pytest
from jsalchemy_web_context import db
from sqlalchemy import select


@pytest.mark.asyncio
async def test_user_groups(context, auth, users):

    async with context() as ctx:
        user = await db.get(auth.user_model, 1)
        user_groups = await auth._user_groups(1)
        assert user_groups == {1}
        admin = auth.group_model(name='admin')
        await db.flush()
        (await user.awaitable_attrs.memberships).append(admin)

    async with context() as ctx:
        admin = await db.scalar(select(auth.group_model).where(auth.group_model.name == 'admin'))
        user_groups = await auth._user_groups(1)
        assert user_groups == {1, admin.id}


@pytest.mark.asyncio
async def test_contextual_roles(context, auth, users, roles, spatial):
    Country, Department, City = spatial

    async with context() as ctx:
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice_group = await db.scalar(select(auth.group_model).where(auth.group_model.name == 'alice'))

        assert alice_group is not None
        assert france is not None

        await auth.grant(alice_group, 'admin', france)

    async with context() as ctx:
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice_group = await db.scalar(select(auth.group_model).where(auth.group_model.name == 'alice'))
        admin_role = await db.scalar(select(auth.role_model).where(auth.role_model.name == 'admin'))

        roles = await auth._contextual_roles(alice_group.id,
                                             auth._resolve_context(france))

        assert admin_role.id in roles

        await auth.grant(alice_group, 'editor', france)

    async with context() as ctx:
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice_group = await db.scalar(select(auth.group_model).where(auth.group_model.name == 'alice'))

        roles = await auth._contextual_roles(alice_group.id,
                                             auth._resolve_context(france))

        role_names = set((await db.execute(select(auth.role_model.name).where(auth.role_model.id.in_(roles)))).scalars().all())
        assert role_names == {'admin', 'editor'}

@pytest.mark.asyncio
async def test_grants(spatial, context, auth, roles, users):
    from jsalchemy_auth.auth import PermissionGrantError
    from jsalchemy_auth.auth import rolegrant
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

        all_grants = set((await db.execute(select(rolegrant))).all())
        assert len(all_grants) == 3

        role_ids = {role.name: role.id for role in (await db.execute(select(auth.role_model))).scalars().all()}
        assert (alice.id, role_ids['admin'], 1, 'country') in all_grants
        assert (bob.id, role_ids['read-only'], 1, 'country') in all_grants
        assert (charlie.id, role_ids['editor'], 1, 'country') in all_grants

@pytest.mark.asyncio
async def test_revoke(auth, spatial, context, roles, users):
    from jsalchemy_auth.auth import PermissionGrantError
    from jsalchemy_auth.auth import rolegrant
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

        all_grants = set((await db.execute(select(rolegrant))).all())
        assert len(all_grants) == 3

        role_ids = {role.name: role.id for role in (await db.execute(select(auth.role_model))).scalars().all()}
        assert (alice.id, role_ids['admin'], 1, 'country') in all_grants
        assert (bob.id, role_ids['read-only'], 1, 'country') in all_grants
        assert (charlie.id, role_ids['editor'], 1, 'country') in all_grants

    async with context() as ctx:
        alice = await db.get(auth.group_model, 1)
        bob = await db.get(auth.group_model, 2)
        charlie = await db.get(auth.group_model, 3)
        await auth.revoke(alice, 'admin', auth._resolve_context(await db.get(Country, 1)))
        await auth.revoke(bob, 'read-only', auth._resolve_context(await db.get(Country, 1)))
        await auth.revoke(charlie, 'editor', auth._resolve_context(await db.get(Country, 1)))

        all_grants = set((await db.execute(select(rolegrant))).all())
        assert len(all_grants) == 0

@pytest.mark.asyncio
async def test_permissions(auth, spatial, context, roles, users):
    Country, Department, City = spatial

    async with context() as ctx:
        italy = await db.scalar(select(Country).where(Country.name == 'Italy'))
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice = await db.get(auth.user_model, 1)
        bob = await db.get(auth.user_model, 2)
        charlie = await db.get(auth.user_model, 3)
        alice_group = (await alice.awaitable_attrs.memberships)[0]
        bob_group = (await bob.awaitable_attrs.memberships)[0]
        charlie_group = (await charlie.awaitable_attrs.memberships)[0]

        await auth.grant(alice_group, 'admin', italy)
        await auth.grant(bob_group, 'read-only', italy)
        await auth.grant(charlie_group, 'editor', italy)
        await auth.grant(charlie_group, 'read-only', italy)
        await auth.grant(charlie_group, 'read-only', france)

    async with context() as ctx:
        italy = await db.scalar(select(Country).where(Country.name == 'Italy'))
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice = await db.get(auth.user_model, 1)
        bob = await db.get(auth.user_model, 2)
        charlie = await db.get(auth.user_model, 3)
        x = await auth.has_permission(alice, 'create', italy)
        assert x == True
        x = await auth.has_permission(alice, 'create', france)
        assert x == False
        x = await auth.has_permission(bob, 'read', italy)
        assert x == True
        x = await auth.has_permission(bob, 'read', france)
        assert x == False
        x = await auth.has_permission(bob, 'update', italy)
        assert x == False
        x = await auth.has_permission(bob, 'update', france)
        assert x == False
        x = await auth.has_permission(charlie, 'delete', italy)
        assert x == False
        x = await auth.has_permission(charlie, 'delete', france)
        assert x == False
        x = await auth.has_permission(charlie, 'update', italy)
        assert x == True
        x = await auth.has_permission(charlie, 'update', france)
        assert x == False
        x = await auth.has_permission(charlie, 'read', italy)
        assert x == True
        x = await auth.has_permission(charlie, 'read', france)
        assert x == True

@pytest.mark.asyncio
async def test_can(auth, spatial, context, roles, users):
    Country, Department, City = spatial

    async with context() as ctx:
        italy = await db.scalar(select(Country).where(Country.name == 'Italy'))
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice = await db.get(auth.user_model, 1)
        bob = await db.get(auth.user_model, 2)
        charlie = await db.get(auth.user_model, 3)
        alice_group = (await alice.awaitable_attrs.memberships)[0]
        bob_group = (await bob.awaitable_attrs.memberships)[0]
        charlie_group = (await charlie.awaitable_attrs.memberships)[0]

        await auth.grant(alice_group, 'admin', italy)
        await auth.grant(bob_group, 'read-only', italy)
        await auth.grant(charlie_group, 'editor', italy)
        await auth.grant(charlie_group, 'read-only', italy)
        await auth.grant(charlie_group, 'read-only', france)

    async with context() as ctx:
        italy = await db.scalar(select(Country).where(Country.name == 'Italy'))
        france = await db.scalar(select(Country).where(Country.name == 'France'))
        alice = await db.get(auth.user_model, 1)
        bob = await db.get(auth.user_model, 2)
        charlie = await db.get(auth.user_model, 3)

        assert await auth.can(alice, 'create', italy) == True
        assert await auth.can(alice, 'create', france) == False
        assert await auth.can(bob, 'read', italy) == True
        assert await auth.can(bob, 'read', france) == False
        assert await auth.can(bob, 'update', italy) == False
        assert await auth.can(bob, 'update', france) == False
        assert await auth.can(charlie, 'delete', italy) == False
        assert await auth.can(charlie, 'delete', france) == False
        assert await auth.can(charlie, 'update', italy) == True
        assert await auth.can(charlie, 'update', france) == False
        assert await auth.can(charlie, 'read', italy) == True
        assert await auth.can(charlie, 'read', france) == True

