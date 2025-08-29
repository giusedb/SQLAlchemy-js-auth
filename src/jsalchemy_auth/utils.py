from jsalchemy_web_context import db
from sqlalchemy.orm import DeclarativeBase

from .auth import Context

def to_context(object: DeclarativeBase) -> Context:
    """Convert a DeclarativeBase object to a Context."""
    return Context(object.__tablename__, object.id)
