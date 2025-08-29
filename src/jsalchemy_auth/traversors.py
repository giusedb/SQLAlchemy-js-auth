from jsalchemy_web_context import redis
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm.collections import InstrumentedList, InstrumentedDict

from .auth import Context
from jsalchemy_web_context.manager import redis, db

from .utils import to_context

from marshal import dumps, loads

TABLE_CLASS = None
NAME_TABLE = None

async def to_object(context: Context):
    """Convert a Context to a DeclarativeBase object."""
    if isinstance(context, DeclarativeBase):
        return context
    return await db.get(TABLE_CLASS[context.table], context.id)

def setup(Base):
    """Setup the table resolver dictionaries."""
    global TABLE_CLASS, NAME_TABLE
    TABLE_CLASS = {m.tables[0].name: m.class_ for m in Base.__mapper__.registry.mappers}
    NAME_TABLE = dict(Base.metadata.tables)

async def _referent(object: DeclarativeBase | Context, attribute: str) -> Context:
    """Get the referent of an attribute."""
    context = object if isinstance(object, Context) else to_context(object)
    key = f'traverse:{context.table}.{attribute}'
    blob = await redis.hget(key, context.id)
    if blob:
        context = loads(blob)
        target_context = Context(*context) if type(context) is tuple else context
    else:
        # find the context
        object = await to_object(context)
        if not object:
            await redis.hset(f'traverse:{context.table}.{attribute}', context.id, dumps(None))
            return None
        # TODO it can be optimize by minimizing the query by getting the individual fields
        target = await getattr(object.awaitable_attrs, attribute, None)
        if not target:
            return None
        if isinstance(target, (list, tuple, set, InstrumentedList, InstrumentedDict)):
            target_context = tuple(map(to_context, target))
            target_blob = tuple(map(tuple, target_context))
        elif isinstance(target, DeclarativeBase):
            target_context = to_context(target)
            target_blob = tuple(target_context)
        else:
            target_context = target
            target_blob = target
        await redis.hset(key, context.id, dumps(target_blob))
    return target_context

async def traverse(object: DeclarativeBase, path: str, start:int =0):
    """Iterates across the database objects following the attribute-paths and yield all items from a starting form item `start`"""
    global TABLE_CLASS, NAME_TABLE
    if not TABLE_CLASS:
        setup(object.__class__)
    current = object
    split_path = tuple(path.split("."))
    for n, p in enumerate(split_path, 1):
        current = await getattr(current.awaitable_attrs, p) # _referent(current, p)
        if current is None:
            raise StopAsyncIteration
        if isinstance(current, (InstrumentedList, InstrumentedDict)) or type(current) is tuple:
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
