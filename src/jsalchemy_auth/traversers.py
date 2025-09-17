from itertools import groupby
from operator import itemgetter
from typing import List, Dict, Set, Iterable, AsyncIterable
from marshal import dumps, loads

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, ColumnProperty, RelationshipProperty, MANYTOMANY, ONETOMANY

from jsalchemy_web_context.manager import redis, db

from .utils import Context, to_context, ContextSet

TABLE_CLASS = None
NAME_TABLE = None
CLASS_STRUCTURE = None

def setup_traversers(Base):
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
    # TODO: Use the __mapper__.relationships.<attribute>.synchronize_pairs
    if isinstance(context, Context):
        context = ContextSet(context.model, (context.id,))
    prop = getattr(context.model, attribute).prop
    if isinstance(prop, ColumnProperty):
        target_field = prop.columns[0]
        local_field = context.model.id
        where = local_field.in_(context.ids)
        result = (await db.execute(select(local_field, target_field).where(where))).all()
        return dict(result)
    elif isinstance(prop, (RelationshipProperty)):
        if prop.direction == MANYTOMANY:
            # TODO Many to many shall be tested
            target_table = prop.target.name
            where_field = next(iter(
                c for c in prop.secondary.c
                if c.foreign_keys and any(x.column.table.name == context.table for x in c.foreign_keys)))
            where = where_field.in_(context.ids)
        elif prop.direction == ONETOMANY:
            target_field = next(iter(prop.entity.primary_key))
            local_field = next(iter(prop.remote_side))
            where = local_field.in_(context.ids)
            result = (await db.execute(select(local_field, target_field).where(where))).all()
            return {key: ContextSet(prop.entity.class_, tuple(x[1] for x in grp))
                    for key, grp in groupby(sorted(result), itemgetter(0))}
        else:
            target_field = next(iter(prop.local_columns))
            local_field = context.model.id
            where = local_field.in_(context.ids)
            # target_table = prop.target.name
            # target_field = prop.primaryjoin.right
            # if isinstance(context, Context):
            #     where = prop.parent.tables[0].c.id == context.id
            # else:
            #     where = prop.parent.tables[0].c.id.in_(context.ids)
            result = (await db.execute(select(local_field, target_field).where(where))).all()
            return {key: Context(prop.entity.class_, item) for key, item in result}
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
    return await db.get(context.model, context.id)

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

def _redis_defootprint(is_many: bool, blob: bytes, model: DeclarativeBase = None) -> DeclarativeBase | Context | ContextSet:
    object = loads(blob)
    if blob is None:
        return None
    if is_many:
        if model:
            return ContextSet(model, object)
        return set(object)
    else:
        if model:
            return Context(model, object)
        return object

async def _referent(object: DeclarativeBase | Context | ContextSet, attribute: str) -> (bool, Context | Set[Context]):
    """Get the referent of an attribute."""
    if isinstance(object, DeclarativeBase):
        object = to_context(object)
    contexts = ContextSet(object.model, (object.id,)) if isinstance(object, Context) else object
    key = f'traverse:{contexts.table}.{attribute}'

    is_reference = attribute in contexts.model.__mapper__.relationships
    if is_reference:
        column = contexts.model.__mapper__.relationships[attribute]
    else:
        column = contexts.model.__mapper__.c[attribute]
    is_many = is_reference and column.direction in (MANYTOMANY, ONETOMANY)
    blobs = dict(zip(contexts.ids, await redis.hmget(key, contexts.ids)))
    missing_contexts = ContextSet(contexts.model, tuple(key for key, value in blobs.items() if value is None))
    resolved = {}
    if missing_contexts:
        resolved = await resolve_attribute(contexts, attribute)
        payload = {id: _redis_footprint(target) for id, target in resolved.items()}
        if payload:
            await redis.hset(key, mapping=payload)
    model = column.entity.class_ if is_reference else None
    resolved.update((id, _redis_defootprint(is_many, blob, model))
                    for id, blob in blobs.items() if blob is not None)
    if resolved:
        if is_reference:
            return is_many, ContextSet.join(*resolved.values())
        return is_many, tuple(filter(bool, resolved.values()))
    return is_many, None

def aggregate_references(*references: List[ContextSet | tuple]):
    for type_, objects in groupby(sorted(references, key=lambda x: type(x).__name__), type):
        if type_ is ContextSet:
            yield ContextSet.join(*objects)
        else:
            yield tuple(x[0] for x in objects)

async def traverse(object: DeclarativeBase, path: str, start:int =0, with_depth: bool = False):
    """Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`"""
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
                if depth >= start:
                    yield item
                if depth == seg_len:
                    async for item in tree_traverse(item, subpath, start=start - seg_len):
                        yield item

async def flatten(iterator: AsyncIterable) -> Iterable:
    async for items in iterator:
        for item in items:
            yield item

def class_traverse(cls: DeclarativeBase, path: str):
    """Generates the joins to traverse the class following the attribute-paths"""
    parts = tuple(path.split("."))
    mapper = cls.__mapper__
    for part in parts:
        prop = getattr(mapper.relationships, part, None)
        if prop is None:
            prop = getattr(mapper.attrs, part)
            yield prop
            break
        yield prop
        mapper = prop.mapper


