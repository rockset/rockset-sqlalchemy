import rockset


class Error(rockset.exception.Error):
    @classmethod
    def map_rockset_exception(cls, exc, **kwargs):
        kwargs["message"] = exc.message
        kwargs["code"] = hasattr(exc, "code") and exc.code or None
        kwargs["type"] = hasattr(exc, "type") and exc.type or None
        if type(exc) == rockset.exception.ServerError:
            ret = InternalError(**kwargs)
        elif type(exc) == rockset.exception.NotYetImplemented:
            ret = NotSupportedError(**kwargs)
        elif type(exc) == rockset.exception.AuthError:
            ret = OperationalError(**kwargs)
        elif type(exc) == rockset.exception.LimitReached:
            ret = OperationalError(**kwargs)
        elif type(exc) == rockset.exception.ResourceSuspendedError:
            ret = OperationalError(**kwargs)
        elif type(exc) == rockset.exception.RequestTimeout:
            ret = OperationalError(**kwargs)
        elif type(exc) == rockset.exception.TransientServerError:
            ret = OperationalError(**kwargs)
        elif type(exc) == rockset.exception.InputError:
            ret = ProgrammingError(**kwargs)
        else:
            ret = cls(**kwargs)
        return ret


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass
