import rockset
from json import loads


class Error(rockset.exceptions.RocksetException):
    @classmethod
    def map_rockset_exception(cls, exc):
        err_body = loads(exc.body)
        args = [err_body["message"], exc.status, err_body["type"]]
        exc_type = type(exc)
        if (
            exc_type == rockset.exceptions.ApiTypeError
            or exc_type == rockset.exceptions.ApiValueError
            or exc_type == rockset.exceptions.ApiAttributeError
            or exc_type == rockset.exceptions.ApiKeyError
            or exc_type == rockset.exceptions.NotFoundException
            or exc_type == rockset.exceptions.InputException
            or exc_type == rockset.exceptions.InitializationException
            or exc_type == rockset.exceptions.BadRequestException
        ):
            ret = ProgrammingError(*args)
        elif (
            exc_type == rockset.exceptions.UnauthorizedException
            or exc_type == rockset.exceptions.ForbiddenException
        ):
            ret = OperationalError(*args)
        elif exc_type == rockset.exceptions.ServiceException:
            ret = InternalError(*args)
        else:
            ret = cls(*args)
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
