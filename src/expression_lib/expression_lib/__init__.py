import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location

lib = _get_shared_lib_location(__file__)


@pl.api.register_expr_namespace("bdt")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr.cast(pl.Int32)


    def add_bday(self, n) -> pl.Expr:
        return self._expr._register_plugin(
            lib=lib,
            symbol="add_bday",
            is_elementwise=True,
            args = [n],
        )
