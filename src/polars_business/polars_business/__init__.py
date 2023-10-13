import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location

lib = _get_shared_lib_location(__file__)

__version__ = "0.1.8"


@pl.api.register_expr_namespace("business")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr.cast(pl.Int32)

    def advance_n_days(self, n, holidays=None) -> pl.Expr:
        # if not (isinstance(n, int) and n > 0):
        #     raise ValueError("only positive integers are currently supported for `n`")
        if holidays is None:
            return self._expr._register_plugin(
                lib=lib,
                symbol="advance_n_days",
                is_elementwise=True,
                args=[
                    n,
                ],
            )
        else:
            return self._expr._register_plugin(
                lib=lib,
                symbol="advance_n_days",
                is_elementwise=True,
                args=[n, pl.Series([list(set(holidays))]).cast(pl.List(pl.Int32))],
            )
