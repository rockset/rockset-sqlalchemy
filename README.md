# rockset-sqlalchemy
This library implements Python's dbapi spec and provides SQLAlchemy support on top of Rockset.

## Usage
To install the latest version of this package, run:

    pip3 install rockset-sqlalchemy

To connect to the database:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "rockset://",
    connect_args={
        "api_key": "{your api key}",
        "api_server": "{your api server}"
        "virtual_instance": "{your virtual instance ID}" # virtual_instance is optional
    },
)
```

See some example queries [here](https://github.com/rockset/rockset-sqlalchemy/blob/main/example.py). See the SQLAlchemy Unified Tutorial [here](https://docs.sqlalchemy.org/en/20/tutorial/index.html).

## Development
Iterating on this library is very simple.

First, clone the source repository:

    git clone https://github.com/rockset/rockset-sqlalchemy

Then, all you need to do is run `sudo python3 setup.py develop` from the cloned directory and hack away.

You can use the example script `example.py` to get started with development. Make sure you provide a `ROCKSET_API_KEY` and `ROCKSET_API_SERVER` to the script, like so

```
ROCKSET_API_KEY=xxx ROCKSET_API_SERVER=https://api.rs2.usw2.rockset.com python3 example.py
```
