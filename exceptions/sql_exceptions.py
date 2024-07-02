
class DatabaseError(Exception):
    pass


class PrimaryKeyViolationError(DatabaseError):
    pass
