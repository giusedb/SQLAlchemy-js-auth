from itertools import groupby
from operator import itemgetter
from typing import Dict, List

from sqlalchemy import Table
from typing_extensions import NamedTuple

from jsalchemy_web_context import db
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty

from jsalchemy_web_context.cache import memoize_one


class Context(NamedTuple):
    table: str
    id: int

    def __add__(self, other):
        if isinstance(other, ContextSet):
            return ContextSet(self.table, (self.id,) + other.ids)
        if isinstance(other, Context):
            return ContextSet(self.table, (self.id, other.id))
        return ContextSet(self.table, (self.id, other))

class ContextSet(NamedTuple):
    table: str
    ids: tuple[int]

    class ContextSetIterator:

        def __init__(self, table, ids):
            self.table = table
            self.ids = ids
            self.index = -1
            self.length = len(ids) - 1

        def __next__(self):
            if self.index < self.length:
                self.index += 1
                return Context(self.table, self.ids[self.index])
            raise StopIteration

    def __bool__(self):
        return bool(self.ids)

    def __len__(self):
        return len(self.ids)

    def __iter__(self):
        return iter((Context(self.table, id) for id in self.ids))

    def __add__(self, other):
        if isinstance(other, ContextSet):
            if self.table != other.table:
                raise ValueError("ContextSet tables must match")
            return ContextSet(self.table, self.ids + other.ids)
        if isinstance(other, Context):
            if self.table != other.table:
                raise ValueError("ContextSet tables must match")
            return ContextSet(self.table, self.ids + [other.id])
        return ContextSet(self.table, self.ids + [other])

    def __iter__(self):
        return self.ContextSetIterator(self.table, self.ids)

    def __contains__(self, item):
        if isinstance(item, Context):
            if item.table != self.table:
                return False
            return item.id in self.ids
        return item in self.ids

    @staticmethod
    def join(*contexts):
        if not contexts:
            raise ValueError("ContextSet.join requires at least one context")
        if len({c.table for c in contexts}) != 1:
            raise ValueError("ContextSet.join requires contexts with the same table")

        ids = set()
        for context in contexts:
            if isinstance(context, ContextSet):
                ids.update(context.ids)
            elif isinstance(context, Context):
                ids.add(context.id)
        ret = ContextSet(contexts[0].table, tuple(filter(bool,ids)))
        if len(ret.ids):
            return ret
        return None


def to_context(object: DeclarativeBase) -> Context:
    """Convert a DeclarativeBase object to a Context."""
    if isinstance(object, (Context, ContextSet)):
        return object
    return Context(object.__tablename__, object.id)

async def to_object(context: Context) -> DeclarativeBase:
    """Convert a Context to a DeclarativeBase object."""
    return await db.get(context.table, context.id)

def invert_relation(relation: RelationshipProperty):
    from jsalchemy_auth.traversers import CLASS_STRUCTURE
    inv_property_name = relation.back_populates
    if not inv_property_name:
        middle_column = property.primaryjoin.right.name
        inv_property_name = {name for name, prop in CLASS_STRUCTURE[relation.target.name].items()
                             if isinstance(prop, RelationshipProperty)
                             and prop.primaryjoin.right.name == middle_column}
    return relation.target.name, inv_property_name

def inverted_properties(schema: Dict[str, List[str]]):
    """Inverts the properties of a dictionary."""
    from jsalchemy_auth.traversers import CLASS_STRUCTURE
    ret = []
    all_relations = tuple((table_name, property_name)
                     for table_name, properties in schema.items()
                     for property_name in properties)
    for table_name, property_name in all_relations:
        relation = CLASS_STRUCTURE[table_name][property_name]
        if isinstance(relation, RelationshipProperty):
            ret.append(invert_relation(relation))
    return {tab: {x[1] for x in grp} for tab, grp in groupby(sorted(ret), itemgetter(0))}

def table_to_class(Base, table: str):
    """Resolve any table or table name to a `DeclarativeBase` model class."""
    if isinstance(table, Table):
        return next(iter(mapper.class_
                         for mapper in Base.registry.mappers
                         if table in mapper.tables))
    return next(iter(mapper.class_
                     for mapper in Base.registry.mappers
                     if any(tab.name == table for tab in mapper.tables)))

def get_target_table(query):
    """find the target of a query"""
    target = query.froms[0]
    if isinstance(target, Table):
        return target
    ret = {x.table for x in target.exported_columns}
    if len(ret) != 1:
        raise ValueError("Query has multiple tables")
    return ret.pop()
