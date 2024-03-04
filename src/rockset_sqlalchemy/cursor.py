from datetime import datetime, date
import json

import rockset

from .exceptions import Error, ProgrammingError


class Cursor(object):
    def __init__(self, connection):
        self._connection = connection
        self._closed = False
        self.arraysize = 1
        self._response = None
        self._response_iter = None

    @staticmethod
    def __convert_to_rockset_type(v):
        if isinstance(v, bool):
            return "bool"
        elif isinstance(v, int):
            return "int"
        elif isinstance(v, float):
            return "float"
        elif isinstance(v, str):
            return "string"
        # this check needs to be first because `date` is also a `datetime`
        elif isinstance(v, datetime):
            return "datetime"
        elif isinstance(v, date):
            return "date"
        elif isinstance(v, dict) or isinstance(v, list):
            return "object"
        elif v is None:
            return "null"
        raise TypeError(
            "Parameter value of type {} is not supported by Rockset".format(type(v))
        )

    @staticmethod
    def execute_query(client, query, vi=None, query_params={}):
        request = rockset.models.QueryRequestSql(
            query=query,
            parameters=[
                rockset.models.QueryParameter(
                    name=param,
                    value=str(val),
                    type=Cursor.__convert_to_rockset_type(val),
                )
                for param, val in query_params.items()
            ],
        )
        try:
            return (
                client.VirtualInstances.query_virtual_instance(
                    virtual_instance_id=vi, sql=request
                )
                if vi
                else client.Queries.query(sql=request)
            )
        except rockset.exceptions.RocksetException as e:
            raise Error.map_rockset_exception(e)

    def execute(self, sql, parameters=None):
        self.__check_cursor_opened()

        # Serialize all list parameters to strings.
        new_params = {}
        if parameters:
            for k, v in parameters.items():
                if isinstance(v, list):
                    new_params[k] = json.dumps(v)
                else:
                    new_params[k] = v
        parameters = new_params

        if self._connection.debug_sql:
            print("+++++++++++++++++++++++++++++")
            print(f"Query:\n{sql}")
            print(f"\nParameters:\n{parameters}")
            print("+++++++++++++++++++++++++++++")

        if parameters and not isinstance(parameters, dict):
            raise ProgrammingError(
                "Unsupported type for query parameters: expected `dict`, found {}".format(
                    type(parameters)
                )
            )

        self._response = Cursor.execute_query(
            self._connection._client, sql, self._connection.vi, query_params=parameters
        )
        self._response_iter = iter(self._response.results)

    def executemany(self, sql, all_parameters):
        for parameters in all_parameters:
            self.execute(sql, parameters)

    def fetchone(self):
        self.__check_cursor_opened()

        try:
            next_doc = next(self._response_iter)
        except StopIteration:
            return None
        except rockset.exceptions.RocksetException as e:
            raise Error.map_rockset_exception(e)

        if not next_doc:
            return None

        result = []

        for field in self._response_to_column_fields(self._response.column_fields):
            name = field["name"]
            if name in next_doc:
                result.append(next_doc[name])
            else:
                result.append(None)

        return tuple(result)

    def _response_to_column_fields(self, column_fields):
        # find the data type of each column by looking at
        # the result set.
        if column_fields:
            columns = [cf["name"] for cf in column_fields]

        schema = rockset.Document()
        if self._response.results and len(self._response.results) > 0:
            # we only look at the first document because
            # is sqlalchemy is typically used for relational
            # tables with no sparse fields
            schema.update(self._response.results[0])

        return schema.fields(columns=columns)

    def fetchall(self):
        docs = []
        while True:
            doc = self.fetchone()
            if doc is None:
                break
            docs.append(doc)
        return docs

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        docs = []
        while len(docs) != size:
            doc = self.fetchone()
            if doc is None:
                break
            docs.append(doc)
        return docs

    @property
    def description(self):
        if self._response is None:
            return None

        desc = []
        for field_name, field_value in self._response.results[0].items():
            name, type_ = field_name, Cursor.__convert_to_rockset_type(field_value)
            null_ok = name != "_id" and "__id" not in name

            # name, type_code, display_size, internal_size, precision, scale, null_ok
            desc.append((name, type_, None, None, None, None, null_ok))
        return desc

    def __iter__(self):
        return self

    def __next__(self):
        next_doc = self.fetchone()
        if next_doc is None:
            raise StopIteration
        else:
            return next_doc

    next = __next__

    def close(self):
        self._closed = True
        self._response = None

    @property
    def rowcount(self):
        self.__check_cursor_opened()
        return len(self._response.results)

    def __check_cursor_opened(self):
        if self._connection._closed:
            raise ProgrammingError("Connection is closed")
        if self._closed:
            raise ProgrammingError("Cursor is closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
