from .connection import Connection as connect
from .exceptions import Error

__all__ = ["connect", "Error"]

apilevel = "2.0"

# TODO: is 2 the right choice here? See https://www.python.org/dev/peps/pep-0249/#threadsafety
threadsafety = 2

# Rockset supports named parameters. See https://www.python.org/dev/peps/pep-0249/#paramstyle
paramstyle = "named"
