from sqlalchemy.engine import default
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler
import rockset
from rockset import Client


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


class EverythingSet(object):
    def __contains__(self, _):
        return True


class RocksetIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade.
    reserved_words = EverythingSet()


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


class RocksetDialect(default.DefaultDialect):

    name = "rockset"
    driver = "rockset"

    positional = False
    paramstyle = "named"

    statement_compiler = compiler.SQLCompiler
    type_compiler = compiler.GenericTypeCompiler
    preparer = RocksetIdentifierPreparer
    default_schema_name = "commons"

    supports_alter = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    preexecute_autoincrement_sequences = False

    supports_default_values = False
    supports_sequences = False
    supports_native_enum = False
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        from rockset_sqlalchemy import client

        return client

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        return [w["name"] for w in connection.connect()._client.Workspace.list()]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            schema = RocksetDialect.default_schema_name
        return [
            w["name"]
            for w in connection.connect()._client.Collection.list(workspace=schema)
        ]

    def _get_table_columns(self, connection, table_name, schema):
        schema = self.identifier_preparer.quote_identifier(schema)
        table_name = self.identifier_preparer.quote_identifier(table_name)

        # Get a single row and determine the schema from that.
        # This assumes the whole collection has a fixed schema of course.
        q = "SELECT * FROM {}.{} LIMIT 1".format(schema, table_name)
        try:
            cursor = connection.connect().connection.cursor()
            cursor.execute(q)
            fields = cursor.description
            if not fields:
                # Return a fake schema if the collection is empty.
                return [("null", "null")]
            field_type = fields[1]
            if field_type not in type_map:
                raise exc.SQLAlchemyError(
                    "Query returned unsupported type {} in field {} in table {}.{}".format(
                        field_type, fields[0], schema, table_name
                    )
                )
            return [
                {
                    "name": field[0],
                    "type": field[1],
                    "nullable": field[6],
                    "default": None,
                }
                for field in fields
            ]
        except Exception as e:
            # TODO: more graceful handling of exceptions.
            raise e

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = RocksetDialect.default_schema_name
        columns = []
        for column in self._get_table_columns(connection, table_name, schema):
            columns.append(column)
        return columns

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        # TODO: implement this.
        return []

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Rockset does not have foreign keys.
        return []

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": ["_id"], "name": "_id_pk"}

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def do_rollback(self, dbapi_connection):
        # Transactions are not supported in Rockset.
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True
