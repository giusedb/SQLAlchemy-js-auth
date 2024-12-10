from .base import BasePermission


class Ownership(BasePermission):
    """Defines a special permission that indicates that a user is an owner of a DB object.

    It implies that an attribute on the target object is the ID of the user."""

    def __init__(self, field: str, path: str = ''):
        pass



class GroupOwnership(BasePermission):
    """Defines a special permission that indicates that a `group` is an owner of a DB object.

    It implies that an attribute on the target object is the ID of the intended `group`."""

    def __init__(self, field: str, path: str = ''):
        pass
