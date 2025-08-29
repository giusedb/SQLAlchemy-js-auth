# SQLAlchemy-js-authorization

SQLAlchemy-js-authorization is a library for managing permissions and roles in a database using python and SQLAlchemy.
It works in asynchronous environment via asyncio.

## Key Concepts

- **Context**: Any object stored in the database. A permission can be checked against any record in the database.
- **RoleGrant**: Represents the association of a `UserGroup`, a `Role`, and a context. It is defined by:
  - `user_group_id`: Integer
  - `role_id`: Integer
  - `table`: String (name of the database table)
  - `pk`: String (primary key of the record in the table)

- **Permission**: A model class representing a permission associated with a role. It has:
  - `name`: String (unique)
  - `is_global`: Boolean
  - `roles`: Many-to-many relationship with `Role` through the "role_permission" table

- **UserGroup**: A group of users sharing access permissions. Roles are granted to user groups in specific contexts.

- **User**: Represents an application user who can log in and perform actions based on granted roles and permissions. It has:
  - `id`: Integer (primary key)
  - `username`: String
  - `password`: String
  - `groups`: Many-to-many relationship with `UserGroup` through a membership table

- **Role**: A set of permissions assigned to users or groups in specific contexts. It has:
  - `id`: Integer (primary key)
  - `name`: String (unique)
  - `is_global`: Boolean
  - `permissions`: Many-to-many relationship with `Permission` through the "role_permission" table
  - `usergroups`: Many-to-many relationship with `UserGroup` through the "rolegrant" table
  - `tables`: String is a comma-separated list of tables that which the role is associated with
 

## Relationships

| Model       | Relationships                          |
|-------------|----------------------------------------|
| User       | ← groups → UserGroup                   |
| UserGroup  | ← roles → Role                         |
| Role       | ← permissions → Permission            |
| Role       | ← rolegrants → RoleGrant              |
| RoleGrant  | ← user_group → UserGroup              |
| RoleGrant  | ← role → Role                         |
| RoleGrant  | ← context → (table, pk)               |



## Data traversing

Taking into account that in a SQLAlchemy model each model has relationships between other models, 
we can consider Each model as a node and each relationship as an arc connecting two nodes, resulting in a graph.

Therefore, based on a path (as a dot-separated string) we can specify which nodes to traverse the graph and check the permissions.

For example, if we have a path `a.b.c`, we can traverse the graph from the node `a` to the node `c`, checking the permissions along the way.
The `Traversor` class is responsible for traversing the graph and checking the permissions along the path.

### Permission Propagation

When a role is granted to a context, the role is propagated to the children of the context, following the propagation schema
A propagation schema represnts a subset of datamodels that are affected by the propagation of a role to a context.
The propagation schema is defined by a list of paths to traverse in the datamodel and the permission to apply to the path.

### Permission Checkers

Each permission checker checks the permissions along the path and returns a boolean value indicating whether the user has the permission or not.

- `Ownership`: Checks if the user is the owner of the object targeted by the path.
- `GroupOwnership`: Checks if the group is the owner of the object targeted by the path.
- `Propagated`: Checks if the user has the permission in the object and all its ancestors.
- `Ancestor`: Checks if the user has the permission in one of the ancestors of the object targeted by the path.
## Permission Manager

The `Auth` class provides methods to manage roles, permissions, and access control.

### Key Methods

1. **assign(permission_name: string: str, role_name: List[str])**  
   Assigns a permission to a role.

2. **unassign(permission_name: str, role_name: str)**  
   Removes a permission from a role.

3. **grant(user_group: UserGroup, role_name: str, context: BaseModel)**  
   Grants a role to a `UserGroup` in the context of a specific database record.
   It raises a PermissionGrantError if the role cannot be granted to the table used in the context.

4. **revoke(user_group: UserGroup, role_name: str, context: BaseModel)**  
   Revokes a role from a `UserGroup` in the context of a specific database record.

5. **can(user: User, permission_name: str, context: BaseModel)**  
   Checks if a user has the specified permission in the given context.
