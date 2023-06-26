import json

from sqlalchemy import exc, types, util
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler

from .compiler import RocksetCompiler
from .types import type_map


class EverythingSet(object):
    def __contains__(self, _):
        return True


class RocksetIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade.
    reserved_words = EverythingSet()


class RocksetDialect(default.DefaultDialect):
    name = "rockset"
    driver = "rockset"

    positional = False
    paramstyle = "named"

    statement_compiler = RocksetCompiler
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

    def _json_deserializer(self, j):
        return j

    def _json_serializer(self, j):
        return j
