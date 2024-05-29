import os

import pytest
from sqlalchemy import create_engine

# unittests are local so ROCKSET_API_KEY can be set to anything
host = "https://api.usw2a1.rockset.com"
api_key = "abc123"


@pytest.fixture
def engine():

    return create_engine(
        "rockset://",
        connect_args={
            "api_server": host,
            "api_key": api_key,
        },
    )
