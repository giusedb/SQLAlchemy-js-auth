from itertools import zip_longest, groupby
from operator import itemgetter
from tokenize import group
from typing import List, Dict

from essentials.folders import split_path
from openapidocs.mk.common import is_reference
from sqlalchemy import Column, select

from jsalchemy_web_context import redis
from sqlalchemy.orm import DeclarativeBase, ColumnProperty, RelationshipProperty, MANYTOMANY, ONETOMANY
from sqlalchemy.orm.collections import InstrumentedList, InstrumentedDict

from .auth import Context
from jsalchemy_web_context.manager import redis, db

from .utils import to_context

from marshal import dumps, loads

TABLE_CLASS = None
NAME_TABLE = None
CLASS_STRUCTURE = None

def setup(Base):
    """Setup the table resolver dictionaries."""
    global TABLE_CLASS, NAME_TABLE, CLASS_STRUCTURE
    TABLE_CLASS = {m.tables[0].name: m.class_ for m in Base.__mapper__.registry.mappers}
    NAME_TABLE = dict(Base.metadata.tables)
    CLASS_STRUCTURE = {m.class_.__tablename__: dict(m.class_.__mapper__.attrs) for m in Base.__mapper__.registry.mappers}

def common_path(paths: List[List[str]]) -> Dict[str, Dict | None]:
    """Identifies the common path from a list of list of string."""
    if not paths or not any(paths):
        return None
    grouped = {key: common_path(tuple(x[1:] for x in value))
               for key, value in groupby(filter(bool, paths), itemgetter(0))}
    for k, v in tuple(grouped.items()):
        if v and len(v.items()) == 1:
            grouped['.'.join((k, next(iter(v))))] = next(iter(v.values()))
            del grouped[k]
    return grouped


async def resolve_attribute(context: Context, attribute: str):
    """returns the value of `attribute` for the specified `context`."""
    sqla_attribute = CLASS_STRUCTURE[context.table][attribute]
    if isinstance(sqla_attribute, ColumnProperty):
        target_field = sqla_attribute.columns[0]
        return (await db.execute(select(target_field).where(TABLE_CLASS[context.table].id == context.id))).scalar()
    elif isinstance(sqla_attribute, (RelationshipProperty)):
        if sqla_attribute.direction == MANYTOMANY:
            target_table = sqla_attribute.target.name
            where_field = next(iter(
                c for c in sqla_attribute.secondary.c
                if c.foreign_keys and any(x.column.table.name == context.table for x in c.foreign_keys)))
            where = where_field == context.id
            return tuple(Context(target_table, row.id) for row in await db.execute(select(target_table).where(where)))
        elif sqla_attribute.direction == ONETOMANY:
            target_table = sqla_attribute.target.name
            target_field = sqla_attribute.target.c.id
            where = sqla_attribute.primaryjoin.right == context.id
            return tuple(Context(target_table, id) for id in (await db.execute(select(target_field).where(where))).scalars())
        else:
            target_table = sqla_attribute.target.name
            target_field = sqla_attribute.primaryjoin.right
            where = sqla_attribute.parent.tables[0].c.id == context.id
            item = (await db.execute(select(target_field).where(where))).scalar()
            if item:
                return Context(target_table, item)
    return None

def treefy_paths(*paths: List[str]):
    """Identifies the common path between the paths."""
    split_path = [tuple(path.split(".")) if type(path) is str else path for path in paths]
    return common_path(sorted(split_path))


async def to_object(context: Context):
    """Convert a Context to a DeclarativeBase object."""
    if isinstance(context, DeclarativeBase):
        return context
    return await db.get(TABLE_CLASS[context.table], context.id)


async def _referent(object: DeclarativeBase | Context, attribute: str) -> Context:
    """Get the referent of an attribute."""
    context = object if isinstance(object, Context) else to_context(object)
    key = f'traverse:{context.table}.{attribute}'
    column = CLASS_STRUCTURE[context.table][attribute]
    is_reference = isinstance(column, RelationshipProperty)
    is_many = is_reference and column.direction in (MANYTOMANY, ONETOMANY)
    blob = await redis.hget(key, context.id)
    if blob:
        target = loads(blob)
        if is_many:
            target_table = column.target.name
            target = tuple(Context(target_table, id) for id in target)
        elif is_reference:
            target_table = column.target.name
            target = Context(target_table, target)
    else:
        # find the context
        target = await resolve_attribute(context, attribute)
        if not target:
            await redis.hset(f'traverse:{context.table}.{attribute}', context.id, dumps(None))
            return False, None
        if is_many:
            target_blob = tuple(x.id for x in target)
        else:
            target_blob = target.id if type(target) is Context else target
        await redis.hset(key, context.id, dumps(target_blob))
    return is_many, target

async def traverse(object: DeclarativeBase, path: str, start:int =0):
    """Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`"""
    global TABLE_CLASS, NAME_TABLE
    if not TABLE_CLASS:
        setup(object.__class__)
    current = object
    split_path = tuple(path.split("."))
    for n, p in enumerate(split_path, 1):
        many, current = await _referent(current, p)
        if current is None:
            raise StopAsyncIteration
        if many:
            for curr in current:
                if start <= n:
                    yield curr
                if len(split_path) > 1:
                    async for c in traverse(curr, '.'.join(split_path[1:]), max(start - 1, 0)):
                        yield c
            break
        else:
            if start <= n:
                yield current

async def tree_traverse(object: DeclarativeBase, *path: str, start:int =0):
    """Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`"""
    for path, subpath in path.items():
        async for item in traverse(object, path, start=start - 1):
            yield item
class PermissionChecker:
    auth: "Auth" = None

    def __or__(self, other):
        return OrPermission(self, other)

    def __repr__(self):
        return f"- [{self.__class__.__name__}] -"

class PathPermission(PermissionChecker):
    def __init__(self,permission: str, *path: List[str], auth: "Auth"=None):
        """Check if the user has the permission for the object following the path."""
        self.permission = permission
        self.paths = treefy_paths(*path) or {}
        self.auth = auth

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        """Check weather at least one of the roles are assigned to """
        async for context in tree_traverse(object, *self.paths):
            for group_id in group_ids:
                context_role_ids = await self.auth._contextual_roles(group_id, context)
                if context_role_ids.intersection(role_ids):
                    return True
        return False

class GlobalPermission(PermissionChecker):

    def __init__(self, permission: str, auth: "Auth"=None):
        self.auth = auth
        self.permission = permission

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        from jsalchemy_auth.auth import GLOBAL_CONTEXT
        if self.permission in await self.auth._global_permissions():
            return await self.auth._has_any_role(group_ids, role_ids)
        for group_id in group_ids:
            global_role_ids = await self.auth._contextual_roles(group_id, GLOBAL_CONTEXT)
            if global_role_ids.intersection(role_ids):
                return True
        return False

class OrPermission(PermissionChecker):
    def __init__(self, *permission_checker):
        self.checkers = permission_checker

    @property
    def auth(self):
        return self.checkers[0].auth

    @auth.setter
    def auth(self, auth: "Auth"):
        for checker in self.checkers:
            checker.auth = auth

    async def __call__(self, group_ids: Set[int], role_ids: Set[int], object: DeclarativeBase) -> bool:
        context = to_context(object)
        for checker in self.checkers:
            if await checker(group_ids, role_ids, context):
                return True
        return False

