

class Permission:
    def __init__(self, auht: "Auth", permission_name: str, path: str):
        auth = auht
        self.permission_name = permission_name
        self.path = path

    async def is_authorized(self, user: UserMixin, context: Context | DeclarativeBase):
        return await self.auth.has_permission(user, self.permission_name, context)