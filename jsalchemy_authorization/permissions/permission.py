from .base import BasePermission


class Permission(BasePermission):
    """Define an explicit permission directly onto whether database object or a linked one."""

    def __init__(self, permission: str, path: str):
        pass
    