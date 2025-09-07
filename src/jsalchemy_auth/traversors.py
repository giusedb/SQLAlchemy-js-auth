from itertools import groupby
from operator import itemgetter
from typing import List, Dict, Set, Iterable, AsyncIterable
from marshal import dumps, loads

from sqlalchemy import Column, select, false
from sqlalchemy.engine.reflection import cache
from sqlalchemy.orm import DeclarativeBase, ColumnProperty, RelationshipProperty, MANYTOMANY, ONETOMANY, MANYTOONE

from jsalchemy_web_context.manager import redis, db, request

from .utils import Context, to_context, ContextSet

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

async def resolve_attribute(context: ContextSet | Context, attribute: str) -> dict:
    """returns the value of `attribute` for the specified `context`."""
    sqla_attribute = CLASS_STRUCTURE[context.table][attribute]
    if isinstance(context, Context):
        context = ContextSet(context.table, (context.id,))
    if isinstance(sqla_attribute, ColumnProperty):
        target_field = sqla_attribute.columns[0]
        source_id = TABLE_CLASS[context.table].id
        stmt = select(source_id, target_field).where(source_id.in_(context.ids))
        return dict((await db.execute(stmt)).all())
    elif isinstance(sqla_attribute, (RelationshipProperty)):
        if sqla_attribute.direction == MANYTOMANY:
            # TODO Many to many shall be tested
            target_table = sqla_attribute.target.name
            where_field = next(iter(
                c for c in sqla_attribute.secondary.c
                if c.foreign_keys and any(x.column.table.name == context.table for x in c.foreign_keys)))
            where = where_field.in_(context.ids)
        elif sqla_attribute.direction == ONETOMANY:
            context_table = sqla_attribute.target.name
            pj = sqla_attribute.primaryjoin
            stmt = (select(pj.right, pj.right.table.c.id)
                    .where(pj.right.in_(context.ids) if isinstance(context, ContextSet) else pj.right == context.id))
            result = (await db.execute(stmt)).all()
            return {key: ContextSet(context_table, tuple(x[1] for x in group))
                    for key, group in groupby(result, itemgetter(0))}
        else:
            target_table = sqla_attribute.target.name
            target_field = sqla_attribute.primaryjoin.right
            if isinstance(context, Context):
                where = sqla_attribute.parent.tables[0].c.id == context.id
            else:
                where = sqla_attribute.parent.tables[0].c.id.in_(context.ids)
            result = (await db.execute(select(sqla_attribute.parent.tables[0].c.id, target_field).where(where))).all()
            return {key: Context(target_table, item) for key, item in result}
    return {}

def treefy_paths(*paths: List[str]):
    """Identifies the common path between the paths."""
    split_path = [tuple(path.split(".")) if type(path) is str else path for path in paths]
    return common_path(sorted(split_path))

async def to_object(context: Context):
    """Convert a Context to a DeclarativeBase object."""
    if isinstance(context, DeclarativeBase):
        return context
    if isinstance(context, ContextSet):
        return tuple(await to_object(c) for c in context)
    return await db.get(TABLE_CLASS[context.table], context.id)

def _redis_footprint(object: DeclarativeBase | Context | ContextSet):
    """Serialize an object to a blob."""
    if isinstance(object, Context):
        to_store = object.id
    elif isinstance(object, ContextSet):
        to_store = set(object.ids)
    elif isinstance(object, DeclarativeBase):
        to_store = object.id
    else:
        to_store = object
    return dumps(to_store)

def _redis_defootprint(is_many: bool, blob: bytes, table: str = None) -> DeclarativeBase | Context | ContextSet:
    object = loads(blob)
    if blob is None:
        return None
    if is_many:
        if table:
            return ContextSet(table, object)
        return set(object)
    else:
        if table:
            return Context(table, object)
        return object


async def _referent(object: DeclarativeBase | Context | ContextSet, attribute: str) -> (bool, Context | Set[Context]):
    """Get the referent of an attribute."""
    if isinstance(object, DeclarativeBase):
        object = to_context(object)
    contexts = ContextSet(object.table, (object.id,)) if isinstance(object, Context) else object
    key = f'traverse:{contexts.table}.{attribute}'
    column = CLASS_STRUCTURE[contexts.table][attribute]
    is_reference = isinstance(column, RelationshipProperty)
    is_many = is_reference and column.direction in (MANYTOMANY, ONETOMANY)
    blobs = dict(zip(contexts.ids, await redis.hmget(key, contexts.ids)))
    missing_contexts = ContextSet(contexts.table, tuple(key for key, value in blobs.items() if value is None))
    resolved = {}
    if missing_contexts:
        resolved = await resolve_attribute(contexts, attribute)
        payload = {id: _redis_footprint(target) for id, target in resolved.items()}
        if payload:
            await redis.hset(key, mapping=payload)
    table_name = column.target.name if is_reference else None
    resolved.update((id, _redis_defootprint(is_many, blob, table_name))
                    for id, blob in blobs.items() if blob is not None)
    if resolved:
        if is_reference:
            return is_many, ContextSet.join(*resolved.values())
        return is_many, tuple(resolved.values())
    return is_many, None

def aggregate_references(*references: List[ContextSet | tuple]):
    for type_, objects in groupby(sorted(references, key=lambda x: type(x).__name__), type):
        if type_ is ContextSet:
            yield ContextSet.join(*objects)
        else:
            yield tuple(x[0] for x in objects)

async def traverse(object: DeclarativeBase, path: str, start:int =0, with_depth: bool = False):
    """Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`"""
    global TABLE_CLASS, NAME_TABLE
    if not TABLE_CLASS:
        setup(object.__class__)
    current = object
    split_path = tuple(path.split("."))
    for n, p in enumerate(split_path, 1):
        many, current = await _referent(current, p)
        if current is None:
            break
        if start <= n:
            yield (current, n) if with_depth else current

async def tree_traverse(object: DeclarativeBase, path: Dict[str, Dict | None], start:int =0, is_root=False):
    """Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`"""
    if is_root:
        yield object
    if path:
        for n, (segment, subpath) in enumerate(path.items()):
            seg_len = segment.count(".") + 1
            async for item, depth in traverse(object, segment, with_depth=True):
                print(f"{' ' * (10 - start + depth)} - {item}")
                if depth >= start:
                    yield item
                if depth == seg_len:
                    async for item in tree_traverse(item, subpath, start=start - seg_len):
                        yield item

async def flatten(iterator: AsyncIterable) -> Iterable:
    async for items in iterator:
        for item in items:
            yield item

