import sys
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

        if parameters:
            if not isinstance(parameters, dict):
                raise ProgrammingError(
                    "Unsupported type for query parameters: expected `dict`, found {}".format(
                        type(parameters)
                    )
                )

        try:
            self._response = self._connection._client.sql(
                query=sql,
                params=parameters
            )
            
            self._response_iter = iter(self._response.results)
        except rockset.exceptions.RocksetException as e:
            raise Error.map_rockset_exception(e)

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
        # if no query has been executed, return None
        if self._response is None:
            return None

        if hasattr(self._response, "column_fields"): # the query was a DESCRIBE call
            desc = []
            for field in self._response_to_column_fields(self._response.column_fields):
                name, type_ = field["name"], field["type"]
                null_ok = name != "_id" and "__id" not in name

                # name, type_code, display_size, internal_size, precision, scale, null_ok
                desc.append((name, type_, None, None, None, None, null_ok))
            return desc
        # the query was a SELECT call
        fields = {}
        for field in self._response.results:
            field_name = field["field"][0]
            if len(field["field"]) == 1 and (field_name not in fields.keys() or fields[field_name][1] is None):
                    # name, type_code, display_size, internal_size, precision, scale, null_ok
                    fields[field_name] = (
                        field_name, 
                        field["type"], 
                        None, None, None, None, 
                        field_name != "_id" and "__id" not in field_name
                    )
        return fields.values()

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
        self._cursor = None

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
