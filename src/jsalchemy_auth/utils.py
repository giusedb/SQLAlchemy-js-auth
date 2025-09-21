from itertools import groupby
from operator import itemgetter
from typing import Dict, List

import sqlalchemy
from sqlalchemy import Table
from typing_extensions import NamedTuple

from jsalchemy_web_context import db
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty, Mapper, registry

from jsalchemy_web_context.cache import memoize_one, memoize_args


class Context(NamedTuple):
    model: DeclarativeBase
    id: int

    @property
    def table(self):
        return self.model.__tablename__ if self.model else 'global'

    def __add__(self, other):
        if isinstance(other, ContextSet):
            return ContextSet(self.table, (self.id,) + other.ids)
        if isinstance(other, Context):
            return ContextSet(self.table, (self.id, other.id))
        return ContextSet(self.table, (self.id, other))

    def __str__(self):
        return f'Context: {self.model.__name__}[{self.id}]'

    __repr__ = __str__

class ContextSet(NamedTuple):
    model: DeclarativeBase
    ids: tuple[int]

    @property
    def table(self):
        return self.model.__tablename__

    class ContextSetIterator:

        def __init__(self, model, ids):
            self.model = model
            self.ids = ids
            self.index = -1
            self.length = len(ids) - 1

        def __next__(self):
            if self.index < self.length:
                self.index += 1
                return Context(self.model, self.ids[self.index])
            raise StopIteration

    def __bool__(self):
        return bool(self.ids)

    def __len__(self):
        return len(self.ids)

    def __iter__(self):
        return iter((Context(self.model, id) for id in self.ids))

    def __add__(self, other):
        if isinstance(other, ContextSet):
            if self.model != other.model:
                raise ValueError("ContextSet tables must match")
            return ContextSet(self.model, self.ids + other.ids)
        if isinstance(other, Context):
            if self.model != other.model:
                raise ValueError("ContextSet models must match")
            return ContextSet(self.model, self.ids + [other.id])
        return ContextSet(self.model, self.ids + [other])

    def __iter__(self):
        return self.ContextSetIterator(self.model, self.ids)

    def __contains__(self, item):
        if isinstance(item, Context):
            if item.model != self.model:
                return False
            return item.id in self.ids
        return item in self.ids

    def __repr__(self):
        return f'CS[{self.model.__name__}: {", ".join(map(str,self.ids))}]'

    __str__ = __repr__

    @staticmethod
    def join(*contexts):
        if not contexts:
            raise ValueError("ContextSet.join requires at least one context")
        if len({c.model for c in contexts}) != 1:
            raise ValueError("ContextSet.join requires contexts with the same model")

        ids = set()
        for context in contexts:
            if isinstance(context, ContextSet):
                ids.update(context.ids)
            elif isinstance(context, Context):
                ids.add(context.id)
        ret = ContextSet(contexts[0].model, tuple(filter(bool,ids)))
        if len(ret.ids):
            return ret
        return None


def to_context(object: DeclarativeBase) -> Context:
    """Convert a DeclarativeBase object to a Context."""
    if isinstance(object, (Context, ContextSet)):
        return object
    return Context(type(object), object.id)

async def to_object(context: Context) -> DeclarativeBase:
    """Convert a Context to a DeclarativeBase object."""
    return await db.get(context.model, context.id)

def inverted_properties(schema: Dict[str, List[str]], registry: sqlalchemy.orm.decl_api.registry):
    """Inverts the properties of a dictionary."""

    def invert_relation(relation: RelationshipProperty):
        inv_property_name = relation.back_populates
        if not inv_property_name:
            middle_column = property.primaryjoin.right.name
            inv_property_name = {name for name, prop in CLASS_STRUCTURE[relation.target.name].items()
                                 if isinstance(prop, RelationshipProperty)
                                 and prop.primaryjoin.right.name == middle_column}
        return relation.entity.class_.__name__, inv_property_name

    idx_mappers = {m.class_.__name__: m for m in registry.mappers}
    idx_mappers.update({m.tables[0].name: m for m in registry.mappers})
    ret = []
    all_relations = tuple((model_name, property_name)
                          for model_name, properties in schema.items()
                          for property_name in properties)
    for model_name, property_name in all_relations:
        mapper = idx_mappers[model_name].mapper
        if property_name in mapper.relationships:
            ret.append(invert_relation(mapper.relationships[property_name]))
    return {tab: {x[1] for x in grp} for tab, grp in groupby(sorted(ret), itemgetter(0))}

@memoize_args
def table_to_class(Base, table: str):
    """Resolve any table or table name to a `DeclarativeBase` model class."""
    if table == 'global':
        return None
    if isinstance(table, Table):
        return next(iter(mapper.class_
                         for mapper in Base.registry.mappers
                         if table in mapper.tables))
    return next(iter(mapper.class_
                     for mapper in Base.registry.mappers
                     if any(tab.name == table for tab in mapper.tables)))

def get_target_table(query):
    """find the target of a query"""
    target = query.get_final_froms()[0]
    if isinstance(target, Table):
        return target
    ret = {x.table for x in target.exported_columns}
    if len(ret) != 1:
        raise ValueError("Query has multiple tables")
    return ret.pop()

def invert_prop(prop: RelationshipProperty):
    if prop.back_populates:
        return prop.entity.relationships[prop.back_populates]
    if isinstance(prop.backref, str):
        return prop.entity.relationships[prop.backref]