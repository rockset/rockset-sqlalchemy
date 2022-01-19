import json
import os
import sys
from collections import defaultdict

import sqlalchemy as sa
from sqlalchemy import cast, create_engine, func, or_
from sqlalchemy.ext import declarative
from sqlalchemy.orm import Query, sessionmaker

from rockset_sqlalchemy.client.sqlalchemy.types import Array

if "ROCKSET_API_SERVER" not in os.environ:
    print("ROCKSET_API_SERVER environment variable not provided!")
    sys.exit(1)

if "ROCKSET_API_KEY" not in os.environ:
    print("ROCKSET_API_KEY environment variable not provided!")
    sys.exit(1)

engine = create_engine(
    "rockset_sqlalchemy://",
    connect_args={
        "api_server": os.environ["ROCKSET_API_SERVER"],
        "api_key": os.environ["ROCKSET_API_KEY"],
    },
)

Base = declarative.declarative_base(bind=engine)


class Person(Base):
    __tablename__ = "people"

    _id = sa.Column(sa.String, primary_key=True)
    name = sa.Column()
    info = sa.Column(sa.JSON)

    def __repr__(self):
        return f"Person(_id={self._id}, info={self.info}, name={self.name})"


"""
INSERT INTO people
SELECT
    'Joe' name,
    JSON_PARSE(
        '{"friends": ["Jack", "Jill"], "favorites": {"lunch": "Sweetgreen", "snack": "Peanut butter cups"}}'
    ) info
UNION ALL
SELECT
    'Jack' name,
    JSON_PARSE(
        '{"friends": ["Mike"], "favorites": {"lunch": "Chipotle", "language": "Python"}}'
    ) info
UNION ALL
SELECT
    'Jill' name,
    JSON_PARSE(
        '{"friends": ["Joe"], "favorites": {"lunch": "Sweetgreen", "language": "C++", "snack": "Pickles"}}'
    ) info
UNION ALL
SELECT
    'Mike' name,
    JSON_PARSE(
        '{"friends": ["Jill"], "favorites": {"lunch": "Subway", "music": "Pop"}}'
    ) info
"""

if __name__ == "__main__":
    session = sessionmaker(bind=engine)()

    q = session.query(Person.info["friends"]).where(Person.name == "Joe")
    results = q.all()[0][0]
    assert len(results) == 2
    assert type(results) == list
    assert results == ["Jack", "Jill"]

    q = session.query(Person.info.op("#>")("friends").op("->>")(1))
    results = set(t[0] for t in q.all())
    assert results == set(["Jack", "Mike", "Joe", "Jill"])

    q = session.query(Person.info.op("#>")("friends"))
    results = q.all()
    d = defaultdict(lambda: 0)
    for r in results:
        for n in r[0]:
            d[n] = d[n] + 1
    assert dict(d) == {"Jill": 2, "Joe": 1, "Mike": 1, "Jack": 1}

    q = session.query(Person.name, Person.info["favorites"])
    results = q.all()
    d = {}
    for r in results:
        name = r[0]
        favs = r[1]
        d[name] = favs
    assert dict(d) == {
        "Jill": {"language": "C++", "lunch": "Sweetgreen", "snack": "Pickles"},
        "Mike": {"music": "Pop", "lunch": "Subway"},
        "Joe": {"lunch": "Sweetgreen", "snack": "Peanut butter cups"},
        "Jack": {"lunch": "Chipotle", "language": "Python"},
    }

    q = session.query(Person.name, Person.info["favorites"]["snack"])
    results = q.all()
    d = {}
    for r in results:
        name = r[0]
        snack = r[1]
        d[name] = snack
    assert dict(d) == {
        "Joe": "Peanut butter cups",
        "Mike": None,
        "Jill": "Pickles",
        "Jack": None,
    }

    # Similar to the query just above, except we subscript using a list.
    q = session.query(Person.name, Person.info[["favorites", "snack"]])
    results = q.all()
    d = {}
    for r in results:
        name = r[0]
        snack = r[1]
        d[name] = snack
    assert dict(d) == {
        "Joe": "Peanut butter cups",
        "Mike": None,
        "Jill": "Pickles",
        "Jack": None,
    }

    q = session.query(Person.name).where(
        Person.info["favorites"]["lunch"] == "Sweetgreen"
    )
    results = set(t[0] for t in q.all())
    assert results == set(["Joe", "Jill"])

    q = session.query(Person.name).where(
        Person.info[["favorites", "lunch"]] == "Sweetgreen"
    )
    results = set(t[0] for t in q.all())
    assert results == set(["Joe", "Jill"])

    q = session.query(Person.name).filter(Person.info[["favorites", "music"]] != None)
    results = set(t[0] for t in q.all())
    assert results == set(["Mike"])

    q = session.query(Person.name).filter(
        or_(
            Person.info[["favorites", "music"]] != None,
            Person.info["friends"][1] == "Jack",
        )
    )
    results = set(t[0] for t in q.all())
    assert results == set(["Mike", "Joe"])
