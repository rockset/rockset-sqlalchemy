import rockset

class Error(rockset.exceptions.RocksetException):
    @classmethod
    def map_rockset_exception(cls, exc, **kwargs):
        kwargs["message"] = exc.message
        kwargs["code"] = hasattr(exc, "code") and exc.code or None
        kwargs["type"] = hasattr(exc, "type") and exc.type or None  
        exc_type = type(exc)
        
        if (
            exc_type == rockset.exceptions.ApiTypeError or
            exc_type == rockset.exceptions.ApiValueError or
            exc_type == rockset.exceptions.ApiAttributeError or
            exc_type == rockset.exceptions.ApiKeyError or 
            exc_type == rockset.exceptions.NotFoundException
        ):
            ret = ProgrammingError(**kwargs)
        elif (
            exc_type == rockset.exceptions.UnauthorizedException or 
            exc_type == rockset.exceptions.ForbiddenException
        ):
            ret = OperationalError(**kwargs)
        elif (
            exc_type == rockset.exceptions.InputException or  
            exc_type == rockset.exceptions.InitializationException or
            exc_type == rockset.exceptions.BadRequestException
        ):
            ret = InputError(**kwargs)
        elif exc_type == ServiceException:
            ret = InternalError(**kwargs)
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
