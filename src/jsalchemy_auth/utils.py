from itertools import groupby
from operator import itemgetter
from typing import Dict, List

from typing_extensions import NamedTuple

from jsalchemy_web_context import db
from sqlalchemy.orm import DeclarativeBase, RelationshipProperty


class Context(NamedTuple):
    table: str
    id: int

def to_context(object: DeclarativeBase) -> Context:
    """Convert a DeclarativeBase object to a Context."""
    if isinstance(object, Context):
        return object
    return Context(object.__tablename__, object.id)

async def to_object(context: Context) -> DeclarativeBase:
    """Convert a Context to a DeclarativeBase object."""
    return await db.get(context.table, context.id)

def invert_relation(relation: RelationshipProperty):
    from jsalchemy_auth.traversors import CLASS_STRUCTURE
    inv_property_name = relation.back_populates
    if not inv_property_name:
        middle_column = property.primaryjoin.right.name
        inv_property_name = {name for name, prop in CLASS_STRUCTURE[relation.target.name].items()
                             if isinstance(prop, RelationshipProperty)
                             and prop.primaryjoin.right.name == middle_column}
    return relation.target.name, inv_property_name

def inverted_properties(schema: Dict[str, List[str]]):
    """Inverts the properties of a dictionary."""
    from jsalchemy_auth.traversors import CLASS_STRUCTURE
    ret = []
    all_relations = tuple((table_name, property_name)
                     for table_name, properties in schema.items()
                     for property_name in properties)
    for table_name, property_name in all_relations:
        relation = CLASS_STRUCTURE[table_name][property_name]
        if isinstance(relation, RelationshipProperty):
            ret.append(invert_relation(relation))
    return {tab: {x[1] for x in grp} for tab, grp in groupby(sorted(ret), itemgetter(0))}
