"""
Database traversal utilities for SQLAlchemy models with Redis caching support.

This module provides functionality to traverse database relationships, cache
results in Redis, and generate optimized queries for complex object graphs.
"""

from itertools import groupby, chain
from operator import itemgetter
from typing import List, Dict, Set, Iterable, AsyncIterable
from marshal import dumps, loads

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, ColumnProperty, RelationshipProperty, MANYTOMANY, ONETOMANY

from jsalchemy_web_context.manager import redis, db

from .utils import Context, to_context, ContextSet


def common_path(paths: List[List[str]]) -> Dict[str, Dict | None]:
    """
    Identifies the common path from a list of list of string.

    Args:
        paths: List of paths represented as lists of strings

    Returns:
        Dictionary representing the common path structure or None if no paths exist
    """
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
    """
    Returns the value of `attribute` for the specified `context`.

    Args:
        context: Context or ContextSet to resolve attribute from
        attribute: Name of the attribute to resolve

    Returns:
        Dictionary mapping IDs to resolved values or empty dict if no resolution possible
    """
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
            result = (await db.execute(select(local_field, target_field).where(where))).all()
            return {key: Context(prop.entity.class_, item) for key, item in result}
    return {}


def treefy_paths(*paths: List[str]):
    """
    Identifies the common path between the paths.

    Args:
        *paths: Variable number of string paths to analyze

    Returns:
        Dictionary representing the common path structure
    """
    split_path = [tuple(path.split(".")) if type(path) is str else path for path in paths]
    return common_path(sorted(split_path))


async def to_object(context: Context):
    """
    Convert a Context to a DeclarativeBase object.

    Args:
        context: Context or ContextSet to convert

    Returns:
        DeclarativeBase object or tuple of objects if context is ContextSet
    """
    if isinstance(context, DeclarativeBase):
        return context
    if isinstance(context, ContextSet):
        return tuple(await to_object(c) for c in context)
    return await db.get(context.model, context.id)


def _redis_footprint(object: DeclarativeBase | Context | ContextSet):
    """
    Serialize an object to a blob for Redis storage.

    Args:
        object: Object to serialize

    Returns:
        Serialized byte representation of the object
    """
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
    """
    Deserialize an object from Redis blob format.

    Args:
        is_many: Flag indicating if the result should be a set or single item
        blob: Serialized data from Redis
        model: Model class to use for deserialization

    Returns:
        Deserialized object or None if blob is None
    """
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
    """
    Get the referent of an attribute.

    Args:
        object: Source object to get referent from
        attribute: Name of the attribute to resolve

    Returns:
        Tuple of (is_many, resolved_object) where is_many indicates if result is collection
    """
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


def aggregate_references(references: List[ContextSet | tuple]):
    """
    Aggregate references by model class.

    Args:
        references: List of reference tuples (model, ids)

    Returns:
        List of ContextSet objects grouped by model class
    """
    # normalize items
    normalized = [(x[0], x[1] if isinstance(x[1], (tuple, list, set)) else (x[1],)) for x in references]
    return [ContextSet(model, tuple(set(chain.from_iterable(map(itemgetter(1), grp)))))
            for model, grp in groupby(sorted(normalized, key=id), itemgetter(0))]


def is_recursive(model: DeclarativeBase, attribute: str) -> bool:
    """
    Check if a relationship is recursive (points to itself).

    Args:
        model: SQLAlchemy model class
        attribute: Name of the relationship attribute

    Returns:
        Boolean indicating if the relationship is recursive
    """
    if attribute not in model.__mapper__.relationships:
        return False
    prop = model.__mapper__.relationships[attribute]
    return prop.target in prop.parent.tables


async def recursive_traverse(object: DeclarativeBase, attribute: str):
    """
    Perform recursive traversal of a relationship.

    Args:
        object: Starting object for traversal
        attribute: Name of the recursive relationship

    Returns:
        Tuple of (has_results, context_set) indicating if results were found
    """
    current = object
    ret = set()
    while True:
        many, current = await _referent(current, attribute)
        if not current:
            break
        ret.add(current)
    if ret:
        return True, ContextSet.join(*ret)
    return False, None


async def traverse(context: ContextSet | Context, path: str, start:int =0, with_depth: bool = False):
    """
    Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`.

    Args:
        context: Starting context for traversal
        path: Dot-separated path string to traverse
        start: Starting depth for yielding results (default: 0)
        with_depth: Whether to yield (item, depth) tuples instead of just items

    Yields:
        Context objects or tuples of (Context, depth) if with_depth is True
    """
    current = context
    split_path = tuple(path.split("."))
    for n, p in enumerate(split_path, 1):
        if is_recursive(current.model, p):
            many, current = await recursive_traverse(current, p)
        else:
            many, current = await _referent(current, p)
        if current is None:
            break
        if start <= n:
            yield (current, n) if with_depth else current


async def tree_traverse(object: DeclarativeBase, path: Dict[str, Dict | None], start:int =0, is_root=False):
    """
    Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`.

    Args:
        object: Starting database object for traversal
        path: Dictionary representing path structure to traverse
        start: Starting depth for yielding results (default: 0)
        is_root: Flag indicating if this is the root node

    Yields:
        Database objects matching the traversal path
    """
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
    """
    Flatten an async iterator of iterables.

    Args:
        iterator: Async iterable containing iterables to flatten

    Yields:
        Individual items from the nested iterables
    """
    async for items in iterator:
        for item in items:
            yield item


def class_traverse(cls: DeclarativeBase, path: str):
    """
    Generates the joins to traverse the class following the attribute-paths.

    Args:
        cls: SQLAlchemy model class to traverse from
        path: Dot-separated path string

    Yields:
        ColumnProperty or RelationshipProperty objects representing the path
    """
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


def all_paths(tree: Dict[str, Dict | None]) -> List[str]:
    """
    Yield all paths in the tree.

    Args:
        tree: Dictionary representing path structure

    Yields:
        String paths from the tree structure
    """
    for key, value in tree.items():
        yield key
        if value:
            yield from (f"{key}.{path}" for path in all_paths(value))


def invert_path(model: DeclarativeBase, path: str):
    """
    Inverts a SQLAlchemy path.

    Args:
        model: SQLAlchemy model class to use for inversion
        path: Dot-separated path string to invert

    Yields:
        RelationshipProperty objects representing the inverted path
    """
    path = list(class_traverse(model, path))
    path.reverse()
    for step in path:
        if isinstance(step, RelationshipProperty):
            yield step.mapper.relationships[step.back_populates]
