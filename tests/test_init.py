"""Test authorization module."""
import pytest
from sqlalchemy import select
# pylint: disable=import-outside-toplevel
# pylint: disable=missing-class-docstring
# pylint: disable=too-few-public-methods
from sqlalchemy.orm import DeclarativeBase, Session, Mapped

from jsalchemy_web_context import db
from src.jsalchemy_auth.auth import Auth


def test_create_tables_basic(sync_db_engine):
    """Test the createion of the necessary tables."""

    class MyBase(DeclarativeBase):
        pass

    auth = Auth(MyBase)

    MyBase.metadata.create_all(sync_db_engine)

    tables = MyBase.metadata.tables

    assert 'users' in tables, 'User class not in metadata'
    assert 'groups' in tables, 'Group class not in metadata'
    assert 'roles' in tables, 'Role class not in metadata'
    assert 'permissions' in tables, 'Permission class not in metadata'
    assert 'roles_permissions' in tables, 'Role permission table not in metadata'
    assert 'rolegrants' in tables, 'Permission permission table not in metadata'
    assert 'memberships' in tables, 'User membership table not in metadata'


@pytest.mark.asyncio
async def test_relations(auth: Auth, context):
    """Test class relationships."""

    async with context():
        dummy = auth.user_model(name='dummy')
        dummy_group = auth.group_model(name='dummy')
        dummy_permission = auth.permission_model(name='dummy')
        dummy_role = auth.role_model(name='dummy')

        db.add_all([dummy, dummy_group, dummy_permission, dummy_role])
        (await dummy.awaitable_attrs.memberships).append(dummy_group)
        (await dummy_role.awaitable_attrs.permissions).append(dummy_permission)

        await db.commit()

        assert dummy_group in dummy.memberships, 'Group not added to user'
        assert dummy in dummy_group.members, 'Group not referenced by user'
        assert dummy_permission in dummy_role.permissions, 'Permission not added to group'
        assert dummy_role in dummy_permission.roles, 'Role not added to group'

@pytest.mark.asyncio
async def test_users(context, auth, user_auth):  # pylint: disable=unused-argument
    """Test from user to permissions."""
    async with context():
        users = {user.name: user for user in (await db.execute(select(auth.user_model))).scalars()}
        groups = {group.name: group for group in (await db.execute(select(auth.group_model))).scalars()}
        roles = {role.name: role for role in (await db.execute(select(auth.role_model))).scalars()}

        assert groups['admin'] in await users['foo'].awaitable_attrs.memberships, 'Group "Admin" added to user'
        assert roles['admin'] in await groups['admin'].awaitable_attrs.granted, 'Role "Admin" added to group'

