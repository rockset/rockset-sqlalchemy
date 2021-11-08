import sqlalchemy as sa
from sqlalchemy import create_engine, func
from sqlalchemy.ext import declarative
from sqlalchemy.orm import sessionmaker
import os
import sys

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


class User(Base):
    __tablename__ = "users"

    _id = sa.Column(sa.String, primary_key=True)
    first_name = sa.Column(sa.String)
    age = sa.Column(sa.Integer)

    def __repr__(self):
        return f"User(_id={self._id}, first_name={self.first_name}, age={self.age})"


class Purchase(Base):
    __tablename__ = "purchases"

    _id = sa.Column(sa.String, primary_key=True)
    user_id = sa.Column(sa.String)
    product = sa.Column(sa.String)
    cost = sa.Column(sa.Float)

    def __repr__(self):
        return f"Purchase(_id={self._id}, user_id={self.user_id}, product={self.product}, cost={self.cost})"


session = sessionmaker(bind=engine)()
cte = (
    sa.select(
        Purchase.user_id.label("user"),
        func.array_agg(Purchase.product).label("products"),
        func.sum(Purchase.cost).label("total_cost"),
    )
    .group_by(Purchase.user_id)
    .cte("user_to_products")
)
results = session.query(User, cte).join(cte, sa.and_(cte.c.user == User._id)).all()
if len(results) > 0:
    for row in results:
        print(row)
else:
    print("No results!")
