import pytest
from sqlalchemy import select, Integer, String, Column, ForeignKey
from sqlalchemy.orm import RelationshipProperty, ColumnProperty, mapped_column, Mapped, relationship

from jsalchemy_auth import Auth
from jsalchemy_auth.auth import GLOBAL_CONTEXT
from jsalchemy_auth.checkers import Path, Owner, Group, Global
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

@pytest.mark.asyncio
async def test_accessible_query(full_people, Person, spatial, context, db_engine, User, Base):
    Country, Department, City = spatial

    auth = Auth(Base, user_model=User,
                actions={
                    'Person': {
                        'read': Path('read', 'city'),
                        'write': Owner(on='city.mayor_id'),
                        'manage': Group(on='city.mayor_id')
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

        assert "FROM person" in str(a_query)
        assert "JOIN city ON city.id = person.city_id" in str(a_query)
        assert "WHERE city.id IN (1)" in str(a_query)

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == {'Jill', 'Joe'}


        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'write', p)}

        a_query = await auth.accessible_query(alice, query, 'write')

        assert "FROM person" in str(a_query)
        assert "JOIN city ON city.id = person.city_id" in str(a_query)
        assert "WHERE city.mayor_id = 1" in str(a_query)

        accessible_people = (await db.execute(a_query)).scalars().all()
        names = {person.name for person in accessible_people}
        assert names == alices_people
        assert names == {'Jane'}

        alices_people = {
            p.name for p in (await db.execute(select(Person))).scalars().all() if await auth.can(alice, 'manage', p)}

        a_query = await auth.accessible_query(alice, query, 'manage')

        assert "FROM person" in str(a_query)
        assert "JOIN city ON city.id = person.city_id" in str(a_query)
        assert "city.mayor_id IN (1000, 1002)" in str(a_query)

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
                        'read': Path('read',
                                               'city', 'city.department', 'city.department.country'),
                        'write': Path('write',
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
        assert names == {'Jane', 'Jill', 'Jack'}

        await auth.grant(bob, 'reader', GLOBAL_CONTEXT)
        await auth.grant(bob, 'editor', await Hobby.get_by_name('Tennis'))

        b_query = await auth.accessible_query(bob, query)
        assert "JOIN city ON city.id = person.city_id" not in str(b_query)
        assert "JOIN department ON department.id = city.department_id" not in str(b_query)
        assert "JOIN country ON country.id = department.country_id" not in str(b_query)
        assert "JOIN job ON job.id = person.job_id" not in str(b_query)
        assert "JOIN hobby ON hobby.id = person.hobby_id" not in str(b_query)


        b_query = await auth.accessible_query(bob, query, 'write')
        bob_people = {p.name for p
                      in (await db.execute(select(Person))).scalars().all()
                      if await auth.can(alice, 'write', p)}
        names = {person.name for person in accessible_people}
        assert "JOIN city ON city.id = person.city_id" not in str(b_query)
        assert "JOIN department ON department.id = city.department_id" not in str(b_query)
        assert "JOIN country ON country.id = department.country_id" not in str(b_query)
        assert "JOIN job ON job.id = person.job_id" not in str(b_query)
        assert "JOIN hobby ON hobby.id = person.hobby_id" in str(b_query)

        assert names == bob_people
        assert names == {'Jane', 'Jill', 'Jack'}


@pytest.mark.asyncio
async def test_accessible_query_branches(full_people, human, Person, spatial, context, db_engine, User, Base):
    Country, Department, City = spatial
    Job, Hobby = human

    class FootballTeam(Base):
        __tablename__ = 'football_team'
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        name: Mapped[str] = mapped_column(String)
        city_id: Mapped[int] = Column(Integer, ForeignKey("city.id"))
        city: Mapped["City"] = relationship("City", backref="football_teams")


    auth = Auth(Base, user_model=User,
                actions={
                    'Person': {
                        'read': Path('read',
                                               'city', 'city.department',
                                               'city.department.country', 'city.football_teams'),
                        'write': Path('write',
                                                'city', 'city.department', 'city.department.country',
                                                'job', 'hobby', 'city.football_teams'),
                    }
                })

    async with db_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with context():
        alice = User(name='alice', last_name='--')
        bob = User(name='bob', last_name='--')
        db.add_all([alice, bob])
        db.add(FootballTeam(name='Milan', city=await City.get_by_name('Milan')))
        db.add(FootballTeam(name='PSG', city=await City.get_by_name('Paris')))
        db.add(FootballTeam(name='Bayern', city=await City.get_by_name('Munich')))


    async with context():
        query = select(Person)
        alice = await User.get_by_name('alice')
        bob = await User.get_by_name('bob')
        all_people = (await db.execute(query)).scalars().all()

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

        await auth.grant(bob, 'reader', await FootballTeam.get_by_name('Bayern'))
        await auth.grant(bob, 'manager', await FootballTeam.get_by_name('PSG'))

        names = {p.name for p in all_people if await auth.can(bob, 'read', p)}
        b_query = await auth.accessible_query(bob, query)
        bob_people = {p.name for p in (await db.execute(b_query)).scalars().all()}
        assert bob_people == names
        assert bob_people == {'Jack', 'Jule'}


        names = {p.name for p in all_people if await auth.can(alice, 'read', p)}
        b_query = await auth.accessible_query(alice, query)
        bob_people = {p.name for p in (await db.execute(b_query)).scalars().all()}
        assert bob_people == names
        assert bob_people == {'Jill', 'Joe'}

        names = {p.name for p in all_people if await auth.can(bob, 'write', p)}
        b_query = await auth.accessible_query(bob, query, 'write')
        bob_people = {p.name for p in (await db.execute(b_query)).scalars().all()}
        assert bob_people == names
        assert bob_people == {'Jule'}


        names = {p.name for p in all_people if await auth.can(alice, 'write', p)}
        a_query = await auth.accessible_query(alice, query, 'write')
        alice_people = {p.name for p in (await db.execute(a_query)).scalars().all()}
        assert alice_people == names
        assert alice_people == set()

        await auth.grant(alice, 'editor', await Person.get_by_name('John'))
        await auth.grant(alice, 'editor', await Country.get_by_name('Germany'))

        names = {p.name for p in all_people if await auth.can(alice, 'write', p)}
        a_query = await auth.accessible_query(alice, query,'write')
        alice_people = {p.name for p in (await db.execute(a_query)).scalars().all()}
        assert alice_people == names
        assert alice_people == {'Jack', 'John'}

@pytest.mark.asyncio
async def test_accessible_query_inverted(Base, full_filesystem, User, db_engine, context):

    build_classes, put_data = full_filesystem

    auth = Auth(Base, user_model=User,
                actions={
                    'MountPoint': {
                        'read': Global('read'),
                    },
                    'Folder': {
                        'read': Path('read', 'mountpoint', 'parent'),
                        'add-tag': Path('add-tag', 'mountpoint', 'parent'),
                    },
                    'File': {
                        'read': Path('read', 'folder'),
                        'add-tag': Path('add-tag', 'folder'),
                    },
                    'Tag': {
                        'read': Path('read', 'files', 'folders'),
                    },
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
        await auth.assign('editor', 'read', 'write')
        await auth.assign('manager', 'read', 'write', 'traverse')

        alice = await User.get_by_name('alice')
        bob = await User.get_by_name('bob')
        root = await User.get_by_name('root')

        users = {user.name: user for user in (alice, bob, root)}

        home = await Folder.get_by_name('home')
        for home in await home.awaitable_attrs.children:
            await auth.grant(users[home.name], 'manager', home)

        await auth.grant(root, 'manager', GLOBAL_CONTEXT)

        all_folders = (await db.execute(select(Folder))).scalars().all()
        all_files = (await db.execute(select(File))).scalars().all()
        all_tags = (await db.execute(select(Tag))).scalars().all()
        all_mount_points = (await db.execute(select(MountPoint))).scalars().all()
        folder_query = select(Folder)

        alices_read_folders = {await f.path for f in all_folders if await auth.can(alice, 'read', f)}
        bob_read_folders = {await f.path for f in all_folders if await auth.can(bob, 'read', f)}

        alice_accessible_folders = await auth.accessible_query(alice, folder_query, 'read')
        bob_accessible_folders = await auth.accessible_query(bob, select(Folder), 'read')

        assert alices_read_folders == {await f.path for f in (await db.execute(alice_accessible_folders)).scalars()}
        assert bob_read_folders == {await f.path for f in (await db.execute(bob_accessible_folders)).scalars()}

        alice_read_files = {await f.path for f in all_files if await auth.can(alice, 'read', f)}
        bob_read_files = {await f.path for f in all_files if await auth.can(bob, 'read', f)}

        alice_accessible_files = await auth.accessible_query(alice, select(File), 'read')
        bob_accessible_files = await auth.accessible_query(bob, select(File), 'read')

        assert alice_read_files == {await f.path for f in (await db.execute(alice_accessible_files)).scalars()}
        assert bob_read_files == {await f.path for f in (await db.execute(bob_accessible_files)).scalars()}

        alice_read_tags = {t.name for t in all_tags if await auth.can(alice, 'read', t)}
        bob_read_tags = {t.name for t in all_tags if await auth.can(bob, 'read', t)}

        alice_accessible_tags = await auth.accessible_query(alice, select(Tag), 'read')
        bob_accessible_tags = await auth.accessible_query(bob, select(Tag), 'read')

        assert alice_read_tags == {t.name for t in (await db.execute(alice_accessible_tags)).scalars()}
        assert bob_read_tags == {t.name for t in (await db.execute(bob_accessible_tags)).scalars()}









