import rockset
from rockset import RocksetClient, Regions

from .cursor import Cursor
from .exceptions import Error, ProgrammingError

class Connection(object):
    def __init__(self, api_server, api_key, debug_sql=False):
        self._closed = False
        self._client = RocksetClient(
            host=api_server, 
            api_key=api_key
        )
        self.debug_sql = debug_sql
        # Used for testing connectivity to Rockset.
        try:
            self._client.Queries.query(
                sql={ "query": "SELECT 1" }
            ).results
        except rockset.exceptions.RocksetException as e:
            raise Error.map_rockset_exception(e)

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
