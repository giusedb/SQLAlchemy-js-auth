"""Test authorization module."""
# pylint: disable=import-outside-toplevel
# pylint: disable=missing-class-docstring
# pylint: disable=too-few-public-methods
from sqlalchemy.orm import DeclarativeBase, Session
from quasar_authorization.auth import Auth


def test_create_tables_basic(db_engine):
    """Test the createion of the necessary tables."""

    class MyBase(DeclarativeBase):
        pass

    Auth(MyBase)

    MyBase.metadata.create_all(db_engine)

    tables = MyBase.metadata.tables

    assert 'auth_user' in tables, 'User class not in metadata'
    assert 'auth_group' in tables, 'Group class not in metadata'
    assert 'auth_role' in tables, 'Role class not in metadata'
    assert 'auth_permission' in tables, 'Permission class not in metadata'
    assert 'auth_role_permission' in tables, 'Role permission table not in metadata'
    assert 'auth_role_assignment' in tables, 'Permission permission table not in metadata'
    assert 'auth_user_membership' in tables, 'User membership table not in metadata'

def test_relations(auth: Auth, session: Session):
    """Test class relationships."""
    dummy = auth.User(username='dummy', password='dummy')
    dummy_group = auth.Group(name='dummy')
    dummy_permission = auth.Permission(name='dummy')
    dummy_role = auth.Role(name='dummy')

    session.add_all([dummy, dummy_group, dummy_permission, dummy_role])
    session.commit()
    dummy.membership.append(dummy_group)
    dummy_role.permissions.append(dummy_permission)

    session.commit()

    assert dummy_group in dummy.membership, 'Group not added to user'
    assert dummy in dummy_group.members, 'Group not referenced by user'
    assert dummy_permission in dummy_role.permissions, 'Permission not added to group'
    assert dummy_role in dummy_permission.roles, 'Role not added to group'


def test_users(auth: Auth, session: Session, user_auth):  # pylint: disable=unused-argument
    """Test from user to permissions."""
    users = {user.username: user for user in session.query(auth.User)}
    groups = {group.name: group for group in session.query(auth.Group)}
    roles = {role.name: role for role in session.query(auth.Role)}

    assert groups['admin'] in users['foo'].membership, 'Group "Admin" added to user'
    assert roles['admin'] in users['foo'].membership, 'Group "Admin" added to user'
