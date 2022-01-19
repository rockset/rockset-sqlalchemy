import rockset
from sqlalchemy import exc, types, util


class BaseType:
    __visit_name__ = None

    def __str__(self):
        return self.__visit_name__


class NullType(BaseType, types.NullType):
    __visit_name__ = rockset.document.DATATYPE_NULL
    hashable = True


class Int(BaseType, types.BigInteger):
    __visit_name__ = rockset.document.DATATYPE_INT


class Float(BaseType, types.Float):
    __visit_name__ = rockset.document.DATATYPE_FLOAT


class Bool(BaseType, types.Boolean):
    __visit_name__ = rockset.document.DATATYPE_BOOL


class String(BaseType, types.String):
    __visit_name__ = rockset.document.DATATYPE_STRING


class Bytes(BaseType, types.LargeBinary):
    __visit_name__ = rockset.document.DATATYPE_BYTES


class Array(NullType):
    __visit_name__ = rockset.document.DATATYPE_ARRAY


class Object(NullType):
    __visit_name__ = rockset.document.DATATYPE_OBJECT


class Date(BaseType, types.DATE):
    __visit_name__ = rockset.document.DATATYPE_DATE


class DateTime(BaseType, types.DATETIME):
    __visit_name__ = rockset.document.DATATYPE_DATETIME


class Time(BaseType, types.TIME):
    __visit_name__ = rockset.document.DATATYPE_TIME


class Time(BaseType, types.String):
    __visit_name__ = rockset.document.DATATYPE_TIMESTAMP


class MicrosecondInterval(BaseType, types.Interval):
    __visit_name__ = rockset.document.DATATYPE_MICROSECOND_INTERVAL

    def bind_processor(self, dialect):
        def process(value):
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            return value

        return process


class MonthInterval(Object):
    __visit_name__ = rockset.document.DATATYPE_MONTH_INTERVAL


class Geography(Object):
    __visit_name__ = rockset.document.DATATYPE_GEOGRAPHY


type_map = {
    rockset.document.DATATYPE_NULL: NullType,
    rockset.document.DATATYPE_INT: Int,
    rockset.document.DATATYPE_FLOAT: Float,
    rockset.document.DATATYPE_BOOL: Bool,
    rockset.document.DATATYPE_STRING: String,
    rockset.document.DATATYPE_BYTES: Bytes,
    rockset.document.DATATYPE_OBJECT: Object,
    rockset.document.DATATYPE_ARRAY: Array,
    rockset.document.DATATYPE_DATE: Date,
    rockset.document.DATATYPE_DATETIME: DateTime,
    rockset.document.DATATYPE_TIME: Time,
    rockset.document.DATATYPE_MICROSECOND_INTERVAL: MicrosecondInterval,
    rockset.document.DATATYPE_MONTH_INTERVAL: MonthInterval,
    rockset.document.DATATYPE_GEOGRAPHY: Geography,
}
