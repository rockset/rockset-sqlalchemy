from sqlalchemy import func
from sqlalchemy.sql import compiler, elements, selectable
from sqlalchemy.sql.operators import custom_op, json_getitem_op, json_path_getitem_op

from .types import Array


class RocksetCompiler(compiler.SQLCompiler):
    def visit_cast(self, cast, **kw):
        if isinstance(cast.type, Array):
            return "CAST({} AS array)".format(self.process(cast.clause, **kw))
        return "CAST({} AS {})".format(
            self.process(cast.clause, **kw), self.process(cast.typeclause)
        )

    def visit_binary(self, b, **kw):
        if isinstance(b.operator, custom_op):
            return self._handle_custom_op(b)
        elif b.operator is json_getitem_op:
            return self._element_at(b)
        elif b.operator is json_path_getitem_op:
            return self._element_at(b)
        return super().visit_binary(b, **kw)

    def _handle_custom_op(self, b):
        if b.operator.opstring == "->":
            return self._element_at(b)
        elif b.operator.opstring == "->>":
            # TODO: Should we use ELEMENT_AT or [] here?
            # Also, should we increment the provided index? Rockset is 1-indexed, but
            # Postgres JSON objects are 0-indexed. Figure this out...
            return "{}[{}]".format(self.process(b.left), self.process(b.right))
        elif b.operator.opstring in ("#>", "#>>"):
            return self._element_at(b)

    def _element_at(self, b):
        right = b.right
        if isinstance(b.right.value, list):
            # This is a tad hacky, but is required because Rockset does not support list parameters.
            # Because of this limitation, we pass in list parameters as JSON strings in cursor.py,
            # and parse the strings back to lists in SQL.
            right = func.JSON_PARSE(b.right)

        # Wrap every ELEMENT_AT with a TRY. This is important especially when accessing a nested
        # field in a JSON, because some level of a JSON can be a non-object, which would then lead
        # to a runtime error.
        return self.process(func.TRY(func.ELEMENT_AT(b.left, right)))

    def get_from_hint_text(self, table, text):
        return text

    def _alter_column_table_in_clause(self, obj):
        if isinstance(obj, elements.BooleanClauseList):
            for clause in obj.clauses:
                self._alter_column_table_in_clause(clause)
        elif isinstance(obj, elements.BinaryExpression):
            for el in [obj.left, obj.right]:
                # If the element that we are visiting is not a column or it's table is not
                # a selectable.TableClause (ie. it's a subquery)
                # or the type of it's name is actually a truncated label (ie. it's an alias)
                # then we don't need to do anything
                if (
                    not isinstance(el, elements.ColumnElement)
                    or not isinstance(el.table, selectable.TableClause)
                    or isinstance(el.table.name, elements._truncated_label)
                ):
                    continue
                effective_schema = self.preparer.schema_for_object(el.table)
                # no effective_schema no issues ! we don't need to alias things
                if not effective_schema:
                    continue
                schema_prefix = self.preparer.quote_schema(effective_schema)
                # no schema prefix same
                if not schema_prefix or schema_prefix == "":
                    continue

                # Ok it's a real table it has schema prefix we need to do something
                # otherwise we would have something like aliasA.col1 = "schema"."tablename".col2
                # and this is not working with our engine ...

                # we just create an alias with the same name as the table,
                # this sounds "dumb" but it's what we need to do to stop sqlalchemy from
                # the table with a schema
                el.table = el.table.alias(el.table.name)

    def visit_join(self, join, asfrom=False, from_linter=None, **kwargs):
        self._alter_column_table_in_clause(join.onclause)
        return super().visit_join(join, asfrom, from_linter, **kwargs)
