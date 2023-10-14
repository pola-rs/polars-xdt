import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location

lib = _get_shared_lib_location(__file__)

__version__ = "0.1.10"


@pl.api.register_expr_namespace("business")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr.cast(pl.Int32)  # hopefully temporary workaround for upstream issue

    def advance_n_days(self, n, weekend=[5,6], holidays=None) -> pl.Expr:
        weekend = pl.Series([list(set(weekend))]).cast(pl.List(pl.Int32))

        if holidays is None:
            return self._expr._register_plugin(
                lib=lib,
                symbol="advance_n_days",
                is_elementwise=True,
                args=[
                    n, weekend
                ],
            )
        else:
            if not isinstance(holidays, list):
                raise ValueError("Expected `holidays` to be a list of datetime.date objects, got: {type(holidays)}")
            return self._expr._register_plugin(
                lib=lib,
                symbol="advance_n_days",
                is_elementwise=True,
                args=[n, weekend, pl.Series([list(set(holidays))]).cast(pl.List(pl.Int32))],
            )
