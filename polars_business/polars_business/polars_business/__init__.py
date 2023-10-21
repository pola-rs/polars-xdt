import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location
from datetime import date

lib = _get_shared_lib_location(__file__)

__version__ = "0.1.20"

mapping = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


@pl.api.register_expr_namespace("business")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def advance_n_days(self, n, weekend=("Sat", "Sun"), holidays=None) -> pl.Expr:
        if not holidays:
            holidays = []
        else:
            holidays = sorted(
                {(holiday - date(1970, 1, 1)).days for holiday in holidays}
            )
        if weekend == ("Sat", "Sun"):
            weekend = [5, 6]
        else:
            weekend = sorted({mapping[name] for name in weekend})
        weekend = [False if i in weekend else True for i in range(7)]

        return self._expr._register_plugin(
            lib=lib,
            symbol="advance_n_days",
            is_elementwise=True,
            args=[n],
            kwargs={
                "holidays": holidays,
                "weekend": weekend,
            },
        )
