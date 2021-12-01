import rockset
from rockset import Client, Q

from .cursor import Cursor
from .exceptions import Error, ProgrammingError


class Connection(object):
    def __init__(self, api_server, api_key):
        self._closed = False
        self._client = Client(api_server=api_server, api_key=api_key)

        # Used for testing connectivity to Rockset.
        try:
            cursor = self._client.sql(Q("SELECT 1"))
            cursor.results()
        except rockset.exception.Error as e:
            raise Error.map_rockset_exception(e)

    def cursor(self):
        if not self._closed:
            return Cursor(self)
        raise ProgrammingError("Connection closed")

    def close(self):
        self._client.close()
        self._closed = True

    def rollback(self):
        # Transactions are not supported in Rockset.
        pass

    def commit(self):
        # Transactions are not supported in Rockset.
        pass


connect = Connection
