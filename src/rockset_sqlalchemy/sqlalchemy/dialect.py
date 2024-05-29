from typing import List, Any, Sequence

from sqlalchemy import exc, types, util, Engine
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
    supports_statement_cache = True

    supports_default_values = False
    supports_sequences = False
    supports_native_enum = False
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        """Retained for backward compatibility with SQLAlchemy 1.x."""
        import rockset_sqlalchemy

        return rockset_sqlalchemy

    @classmethod
    def import_dbapi(cls):
        return RocksetDialect.dbapi()

    def create_connect_args(self, url):
        kwargs = {
            "api_server": "https://{}".format(url.host),
            "api_key": url.password or url.username,
            "virtual_instance": url.database,
        }
        return ([], kwargs)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        return [
            w["name"]
            for w in connection.connect().connection._client.Workspaces.list()["data"]
        ]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        tables = (
            connection.connect().connection._client.Collections.list()
            if schema is None
            else connection.connect().connection._client.Collections.workspace_collections(
                workspace=schema
            )
        )["data"]

        return [w["name"] for w in tables]

    def _get_table_columns(self, connection, table_name, schema):
        """Gets table columns based on a retrieved row from the collection"""
        schema = self.identifier_preparer.quote_identifier(schema)
        table_name = self.identifier_preparer.quote_identifier(table_name)

        # Get a single row and determine the schema from that.
        # This assumes the whole collection has a fixed schema of course.
        q = f"SELECT * FROM {schema}.{table_name} LIMIT 1"
        try:
            fields = self._exec_query_description(connection, q)
            if not fields:
                # Return a fake schema if the collection is empty.
                return [("null", "null")]
            columns = []
            for field in fields:
                field_type = field[1]
                if field_type not in type_map.keys():
                    raise exc.SQLAlchemyError(
                        "Query returned unsupported type {} in field {} in table {}.{}".format(
                            field_type, fields[0], schema, table_name
                        )
                    )
                columns.append(
                    {
                        "name": field[0],
                        "type": type_map[field_type],
                        "nullable": field[6],
                        "default": None,
                    }
                )
        except Exception as e:
            # TODO: more graceful handling of exceptions.
            raise e
        return columns

    def _validate_query(self, connection: Engine, query: str):
        import rockset.models
        query_request_sql = rockset.models.QueryRequestSql(query=query)
        # raises rockset.exceptions.BadRequestException if DESCRIBE is invalid on this collection e.g. rollups
        connection.connect().connection._client.Queries.validate(sql=query_request_sql)

    def _exec_query(self, connection: Engine, query: str) -> Sequence[Any]:
        cursor = connection.connect().connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def _exec_query_description(self, connection: Engine, query: str) -> Sequence[Any]:
        cursor = connection.connect().connection.cursor()
        cursor.execute(query)
        return cursor.description

    def _get_table_columns_describe(self, connection, table_name, schema, **kw):
        """Gets table columns based on the query DESCRIBE SomeCollection"""
        schema = self.identifier_preparer.quote_identifier(schema)
        table_name = self.identifier_preparer.quote_identifier(table_name)

        max_field_depth = kw["max_field_depth"] if "max_field_depth" in kw else 1
        if not isinstance(max_field_depth, int):
            raise ValueError("Query option max_field_depth, must be of type 'int'")

        q = f"DESCRIBE {table_name} OPTION(max_field_depth={max_field_depth})"
        self._validate_query(connection, q)

        try:
            results = self._exec_query(connection, q)
            columns = list()
            for result in results:
                field_name = ".".join(result[0])
                field_type = result[1]
                if field_type not in type_map.keys():
                    raise exc.SQLAlchemyError(
                        "Query returned unsupported type {} in field {} in table {}.{}".format(
                            field_type, field_name, schema, table_name
                        )
                    )
                nullable = False if result[0][0] == "_id" else True  # _id is the only field that's not nullable
                columns.append(
                    {
                        "name": field_name,
                        "type": type_map[result[1]],
                        "nullable": nullable,
                        "default": None,
                    }
                )
        except Exception as e:
            # TODO: more graceful handling of exceptions.
            raise e
        return columns

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = RocksetDialect.default_schema_name
        try:
            return self._get_table_columns_describe(connection, table_name, schema)
        except Exception as e:
            # likely a rollup collection, so revert to old behavior
            pass
        return self._get_table_columns(connection, table_name, schema)

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

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False

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
