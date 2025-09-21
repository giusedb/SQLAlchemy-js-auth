"""Utility functions and classes for handling context and database relationships.

This module provides utilities for converting between database objects and contexts,
managing context sets, and working with SQLAlchemy relationships.
"""

from itertools import groupby
from operator import itemgetter
from typing import Dict, List

import sqlalchemy
from sqlalchemy import Table
from typing_extensions import NamedTuple

from jsalchemy_web_context import db
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty

from jsalchemy_web_context.cache import memoize_args


class Context(NamedTuple):
    """Represents a single context with a model and ID.

    A context is a combination of a DeclarativeBase model class and an ID,
    typically representing a specific database record.
    """

    model: DeclarativeBase
    id: int

    @property
    def table(self):
        """Get the table name for this context."""
        return self.model.__tablename__ if self.model else 'global'

    def __add__(self, other):
        """Combine this context with another context or context set."""
        if isinstance(other, ContextSet):
            return ContextSet(self.table, (self.id,) + other.ids)
        if isinstance(other, Context):
            return ContextSet(self.table, (self.id, other.id))
        return ContextSet(self.table, (self.id, other))

    def __str__(self):
        """String representation of the context."""
        return f'Context: {self.model.__name__}[{self.id}]'

    __repr__ = __str__


class ContextSet(NamedTuple):
    """Represents a set of contexts sharing the same model.

    A context set is a collection of contexts that all refer to records
    of the same model class, useful for batch operations or filtering.
    """

    model: DeclarativeBase
    ids: tuple[int]

    @property
    def table(self):
        """Get the table name for this context set."""
        return self.model.__tablename__

    class ContextSetIterator:
        """Iterator for ContextSet objects."""

        def __init__(self, model, ids):
            self.model = model
            self.ids = ids
            self.index = -1
            self.length = len(ids) - 1

        def __next__(self):
            """Get the next context in the iteration."""
            if self.index < self.length:
                self.index += 1
                return Context(self.model, self.ids[self.index])
            raise StopIteration

    def __bool__(self):
        """Check if the context set is non-empty."""
        return bool(self.ids)

    def __len__(self):
        """Get the number of contexts in this set."""
        return len(self.ids)

    def __iter__(self):
        """Get an iterator over the contexts in this set."""
        return iter((Context(self.model, id) for id in self.ids))

    def __add__(self, other):
        """Combine this context set with another context or context set."""
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
        """Get an iterator over the contexts in this set."""
        return self.ContextSetIterator(self.model, self.ids)

    def __contains__(self, item):
        """Check if a context or ID is contained in this set."""
        if isinstance(item, Context):
            if item.model != self.model:
                return False
            return item.id in self.ids
        return item in self.ids

    def __repr__(self):
        """String representation of the context set."""
        return f'CS[{self.model.__name__}: {", ".join(map(str, self.ids))}]'

    __str__ = __repr__

    @staticmethod
    def join(*contexts):
        """Join multiple contexts into a single context set.

        Args:
            *contexts: Variable number of Context or ContextSet objects

        Returns:
            ContextSet: A new context set containing all contexts
            or None if no valid contexts remain

        Raises:
            ValueError: If no contexts are provided or contexts have
                        different models
        """
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
        ret = ContextSet(contexts[0].model, tuple(filter(bool, ids)))
        if len(ret.ids):
            return ret
        return None


def to_context(object: DeclarativeBase) -> Context:
    """Convert a DeclarativeBase object to a Context.

    Args:
        object: A DeclarativeBase instance or existing Context/ContextSet

    Returns:
        Context: A context representing the object with its model and ID
    """
    if isinstance(object, (Context, ContextSet)):
        return object
    return Context(type(object), object.id)


async def to_object(context: Context) -> DeclarativeBase:
    """Convert a Context to a DeclarativeBase object.

    Args:
        context: A Context object

    Returns:
        DeclarativeBase: The database object represented by the context
    """
    return await db.get(context.model, context.id)


def inverted_properties(schema: Dict[str, List[str]], registry: sqlalchemy.orm.decl_api.registry):
    """Inverts the properties of a dictionary.

    Args:
        schema: Dictionary mapping model names to lists of property names
        registry: SQLAlchemy registry containing mapper information

    Returns:
        Dict[str, set]: Dictionary mapping table names to sets of inverted properties
    """

    def invert_relation(relation: RelationshipProperty):
        """Invert a single relationship property."""
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
    """Resolve any table or table name to a `DeclarativeBase` model class.

    Args:
        Base: The DeclarativeBase instance to search in
        table: The table name or Table object

    Returns:
        DeclarativeBase: The corresponding model class, or None for 'global'
    """
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
    """Find the target of a query.

    Args:
        query: A SQLAlchemy query object

    Returns:
        Table: The target table from the query

    Raises:
        ValueError: If the query has multiple tables
    """
    target = query.get_final_froms()[0]
    if isinstance(target, Table):
        return target
    ret = {x.table for x in target.exported_columns}
    if len(ret) != 1:
        raise ValueError("Query has multiple tables")
    return ret.pop()


def invert_prop(prop: RelationshipProperty):
    """Invert a relationship property.

    Args:
        prop: A RelationshipProperty to invert

    Returns:
        RelationshipProperty: The inverted property or None
    """
    if prop.back_populates:
        return prop.entity.relationships[prop.back_populates]
    if isinstance(prop.backref, str):
        return prop.entity.relationships[prop.backref]
