import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location

lib = _get_shared_lib_location(__file__)

__version__ = "0.1.10"

mapping = {
    'Mon': 0,
    'Tue': 1,
    'Wed': 2,
    'Thu': 3,
    'Fri': 4,
    'Sat': 5,
    'Sun': 6
}


@pl.api.register_expr_namespace("business")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr.cast(pl.Int32)  # hopefully temporary workaround for upstream issue

    def advance_n_days(self, n, weekend=('Sat', 'Sun'), holidays=None) -> pl.Expr:
        if holidays is None and weekend == ('Sat', 'Sun'):
            return self._expr._register_plugin(
                lib=lib,
                symbol="advance_n_days",
                is_elementwise=True,
                args=[n],
            )
        elif holidays is not None and weekend == ('Sat', 'Sun'):
            holidays = pl.Series([[] if holidays is None else list(set(holidays))]).cast(pl.List(pl.Int32))
            return self._expr._register_plugin(
                lib=lib,
                symbol="advance_n_days_w_holidays",
                is_elementwise=True,
                args=[n, holidays],
            )
        else:
            raise NotImplementedError()
