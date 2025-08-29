from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, Table, Text
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Association table for many-to-many relationship between User and UserGroup
user_group_membership = Table(
    'user_group_membership',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('group_id', Integer, ForeignKey('usergroups.id'), primary_key=True)
)

# Association table for many-to-many relationship between Role and Permission
role_permission = Table(
    'role_permission',
    Base.metadata,
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True),
    Column('permission_id', Integer, ForeignKey('permissions.id'), primary_key=True)
)

class User(Base):
    """User model representing an application user."""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(80), unique=True, nullable=False)
    password = Column(String(120), nullable=False)
    
    # Many-to-many relationship with UserGroup
    groups = relationship(
        'UserGroup',
        secondary=user_group_membership,
        back_populates='users',
        lazy='dynamic'
    )

class UserGroup(Base):
    """UserGroup model representing a group of users sharing permissions."""
    __tablename__ = 'usergroups'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(80), unique=True, nullable=False)
    
    # Many-to-many relationship with User
    users = relationship(
        'User',
        secondary=user_group_membership,
        back_populates='groups',
        lazy='dynamic'
    )
    
    # One-to-many relationship with RoleGrant
    role_grants = relationship('RoleGrant', back_populates='user_group')

class Role(Base):
    """Role model representing a set of permissions."""
    __tablename__ = 'roles'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(80), unique=True, nullable=False)
    is_global = Column(Boolean, default=False, nullable=False)
    
    # Many-to-many relationship with Permission
    permissions = relationship(
        'Permission',
        secondary=role_permission,
        back_populates='roles',
        lazy='dynamic'
    )
    
    # One-to-many relationship with RoleGrant
    role_grants = relationship('RoleGrant', back_populates='role')

class Permission(Base):
    """Permission model representing a specific permission."""
    __tablename__ = 'permissions'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(80), unique=True, nullable=False)
    is_global = Column(Boolean, default=False, nullable=False)
    
    # Many-to-many relationship with Role
    roles = relationship(
        'Role',
        secondary=role_permission,
        back_populates='permissions',
        lazy='dynamic'
    )

class RoleGrant(Base):
    """RoleGrant model representing the association of a UserGroup, Role, and context."""
    __tablename__ = 'rolegrants'
    
    id = Column(Integer, primary_key=True)
    
    # Foreign keys
    user_group_id = Column(Integer, ForeignKey('usergroups.id'), nullable=False)
    role_id = Column(Integer, ForeignKey('roles.id'), nullable=False)
    
    # Context information
    table = Column(String(80), nullable=False)  # Name of the database table
    pk = Column(Integer, nullable=False)        # Primary key of the record in the table
    
    # Relationships
    user_group = relationship('UserGroup', back_populates='role_grants')
    role = relationship('Role', back_populates='role_grants')
    
    __table_args__ = (
        # Ensure that a user group can't have the same role in the same context
        # Note: This is a simplified version. In a real implementation, you might need to adjust
        # this based on your specific requirements and database compatibility.
        # Some databases might require different syntax for this constraint.
        # For example, in SQLite you might need to use a different approach.
        # This is a basic implementation that works with most databases.
        # For PostgreSQL, you might want to use Exclude constraint with btree_gist.
        # For SQLite, you might need to use a unique index instead.
        # Please adjust according to your database system.
        # This is a placeholder that works in most cases but might need adjustment.
        # You might want to add this as a separate migration after testing.
        # {'sqlite_autoincrement': True},
        # Uncomment the following line if your database supports it:
        # UniqueConstraint('user_group_id', 'role_id', 'table', 'pk', name='uq_rolegrant_context'),
    )
