import pytest
from sqlalchemy import select

from jsalchemy_auth import Auth
from jsalchemy_auth.checkers import Owner, Path
from jsalchemy_auth.traversers import traverse, invert_path
from jsalchemy_auth.utils import Context, to_context
from jsalchemy_web_context import db


@pytest.mark.asyncio
async def test_owner_permission(Base, spatial, context, users, auth):
    Country, Department, City = spatial
    auth.actions={
        'City': {            'manage': Owner(on='mayor_id')},
        'Department': {
            'manage': Owner(on='president_id')},
        'Country': {
            'manage': Owner(on='president_id')},
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
        'City': {
            'manage': Owner(on='department.country.president_id')},
        'Department': {
            'manage': Owner(on='country.president_id')},
        'Country': {
            'manage': Owner(on='president_id')},
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
        'City': {
            'manage': Owner(on='mayor_id') |
                      Owner(on='department.president_id') |
                      Owner(on='department.country.president_id')},
        'Department': {
            'manage': Owner(on='country.president_id') |
                      Owner(on='president_id')},
        'Country': {
            'manage': Owner(on='president_id')},
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

@pytest.mark.asyncio
async def test_user_recursive_path(Base, full_filesystem, User, db_engine, context):

    build_classes, put_data = full_filesystem

    auth = Auth(Base, user_model=User,
                actions={
                    'Folder': {
                        'read': Path('read', 'parent'),
                    },
                    'File': {
                        'read': Path('read', 'folder.parent.mountpoint'),
                    }
                })

    classes = build_classes()

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    MountPoint, Folder, File, Tag = await put_data(classes)

    async with context():
        alice = User(name='alice', last_name='A', id=1)
        bob = User(name='bob', last_name='B', id=2)
        root = User(name='root', last_name='C', id=3)

        db.add_all([alice, bob, root])

    async with context():
        await auth.assign('reader', 'read')

        alice = await User.get_by_name('alice')
        bob = await User.get_by_name('bob')
        root = await User.get_by_name('root')
        users = {u.name: u for u in [alice, bob, root]}
        home_folder = await Folder.get_by_name('home')

        for folder in await home_folder.awaitable_attrs.children:
            if folder.name in users:
                await auth.grant(users[folder.name], 'reader', folder)
        await auth.grant(bob, 'reader', await MountPoint.get_by_name('root'))

        all_folder = (await db.execute(select(Folder))).scalars().all()
        all_files = (await db.execute(select(File))).scalars().all()

        readable_files = {await file.path for file in all_files if await auth.can(alice, 'read', file)}

        b_query = await auth.accessible_query(bob, select(File), 'read')
        a_query = await auth.accessible_query(alice, select(File), 'read')


        accessible_files = {await file.path for file in (await db.execute(a_query)).scalars().all()}
        assert readable_files == accessible_files

        readable_files = {await file.path for file in all_files if await auth.can(bob, 'read', file)}
        b_query = await auth.accessible_query(bob, select(File), 'read')
        accessible_files = {await file.path for file in (await db.execute(b_query)).scalars().all()}
        assert readable_files == accessible_files


        readable_folders = {await folder.path for folder in all_folder if await auth.can(alice, 'read', folder)}
        assert readable_folders == {'/home/alice/Documents', '/home/alice/Desktop', '/home/alice'}
        a_query = await auth.accessible_query(alice, select(Folder), 'read')
        accessible_folders = {await f.path for f in (await db.execute(a_query)).scalars().all()}
        assert readable_folders == accessible_folders








