from .base import BasePermission


class Propagated(BasePermission):
    """Defines a permission that will be checked along all items along a line defined as `path`."""

    def __init__(self, permission: str, path: str):
        self.permission = permission
        self.path = path
