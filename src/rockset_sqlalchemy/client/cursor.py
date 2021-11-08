from .exceptions import Error, ProgrammingError
import rockset
from rockset.query import QueryStringSQLText


class Cursor(object):
    def __init__(self, connection):
        self._connection = connection
        self._closed = False
        self._cursor = None
        self.arraysize = 1

    def execute(self, sql, parameters=None):
        self.__check_cursor_opened()

        q = QueryStringSQLText(sql)
        if parameters:
            if not isinstance(parameters, dict):
                raise ProgrammingError(
                    "Unsupported type for query parameters: expected `dict`, found {}".format(
                        type(parameters)
                    )
                )
            q.P.update(parameters)

        try:
            self._cursor = self._connection._client.sql(q=q)
            self._cursor_iter = iter(self._cursor.results())
        except rockset.exception.Error as e:
            raise Error.map_rockset_exception(e)

    def executemany(self, sql, all_parameters):
        for parameters in all_parameters:
            self.execute(sql, parameters)

    def fetchone(self):
        self.__check_cursor_opened()

        try:
            next_doc = next(self._cursor_iter)
        except StopIteration:
            return None
        except rockset.exception.Error as e:
            raise Error.map_rockset_exception(e)

        if not next_doc:
            return None

        result = []
        for field in self._cursor.fields():
            name = field["name"]
            if name in next_doc:
                result.append(next_doc[name])
            else:
                result.append(None)

        return tuple(result)

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
        if self._cursor is None:
            return None

        desc = []
        for field in self._cursor.fields():
            name, type_ = field["name"], field["type"]
            null_ok = name != "_id" and "__id" not in name

            # name, type_code, display_size, internal_size, precision, scale, null_ok
            desc.append((name, type_, None, None, None, None, null_ok))
        return desc

    def __iter__(self):
        return self

    next = __next__

    def close(self):
        self._closed = True
        self._cursor = None

    @property
    def rowcount(self):
        self.__check_cursor_opened()
        return self._cursor.rowcount()

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
