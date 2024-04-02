import sys

from sqlalchemy.dialects import registry
from sqlalchemy.sql import and_, column, table
from sqlalchemy.testing import AssertsCompiledSQL


class TestSQL(AssertsCompiledSQL):
    __dialect__ = "rockset"

    def setup_method(self):

        sys.path.insert(0, "./src")

        registry.register("rockset", "rockset_sqlalchemy.sqlalchemy", "RocksetDialect")
        pass

    def test_inner_join_table_on_clause_w_schema(self):
        t1 = table("t1", column("x"), schema="s1")
        t2 = table("t2", column("y"), schema="s2")
        nd = and_(*[column("x") == t2.c.y])
        # the column in the join condition is not part of a table so it shouldn't be prefixed
        self.assert_compile(
            t1.join(t2, nd),
            '"s1"."t1" JOIN "s2"."t2" ON "x" = "t2"."y"',
        )
        nd = and_(*[column("x") == t2.c.y])
        self.assert_compile(
            t1.join(t2, nd),
            '"s1"."t1" JOIN "s2"."t2" ON "x" = "t2"."y"',
        )
        t3 = t2.alias("t3")
        col = column("y")
        col.table = t3
        nd = and_(*[t1.c.x == col])
        # the column in the join condition is not part of a table so it shouldn't be prefixed
        self.assert_compile(
            t1.join(t2, nd),
            '"s1"."t1" JOIN "s2"."t2" ON "t1"."x" = "t3"."y"',
        )

    def test_inner_join_table_on_clause(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("y"))
        nd = and_(*[column("x") == t2.c.y])
        self.assert_compile(
            t1.join(t2.alias("t3"), nd),
            '"t1" JOIN "t2" AS "t3" ON "x" = "t2"."y"',
        )

    def test_inner_join_no_table_on_clause(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("y"))
        nd = and_(*[column("x") == column("y")])
        self.assert_compile(
            t1.join(t2.alias("t3"), nd),
            '"t1" JOIN "t2" AS "t3" ON "x" = "y"',
        )
