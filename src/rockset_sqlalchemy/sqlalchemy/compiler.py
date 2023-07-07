import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.sql import compiler
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
