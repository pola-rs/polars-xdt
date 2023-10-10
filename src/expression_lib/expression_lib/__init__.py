import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location

lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("language")
class Language:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def pig_latinnify(self) -> pl.Expr:
        return self._expr._register_plugin(
            lib=lib,
            symbol="pig_latinnify",
            is_elementwise=True,
        )

    def add_bday(self) -> pl.Expr:
        return self._expr._register_plugin(
            lib=lib,
            symbol="add_bday",
            is_elementwise=True,
            cast_to_supertypes=True
        )
