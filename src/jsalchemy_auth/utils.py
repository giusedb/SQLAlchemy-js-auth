from typing_extensions import NamedTuple

from jsalchemy_web_context import db
from sqlalchemy.orm import DeclarativeBase

class Context(NamedTuple):
    table: str
    id: int


def to_context(object: DeclarativeBase) -> Context:
    """Convert a DeclarativeBase object to a Context."""
    return Context(object.__tablename__, object.id)

async def to_object(context: Context) -> DeclarativeBase:
    """Convert a Context to a DeclarativeBase object."""
    return await db.get(context.table, context.id)
