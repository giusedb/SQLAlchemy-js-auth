"""Define the permission base classess,"""

class BasePermission:
    """Permission Base class."""

    def __and__(self, other):
        raise NotImplementedError()

    def __or__(self, other):
        raise NotImplementedError()

