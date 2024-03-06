from rockset import RocksetClient

from .cursor import Cursor
from .exceptions import ProgrammingError


class Connection(object):
    def __init__(self, api_server, api_key, virtual_instance=None, debug_sql=False):
        self._closed = False
        self._client = RocksetClient(host=api_server, api_key=api_key)
        self.vi = virtual_instance
        self.debug_sql = debug_sql
        # Used for testing connectivity to Rockset.
        Cursor.execute_query(self._client, "SELECT 1", vi=self.vi)

    def cursor(self):
        if not self._closed:
            return Cursor(self)
        raise ProgrammingError("Connection closed")

    def close(self):
        self._client = None
        self._closed = True

    def rollback(self):
        # Transactions are not supported in Rockset.
        pass

    def commit(self):
        # Transactions are not supported in Rockset.
        pass


connect = Connection
