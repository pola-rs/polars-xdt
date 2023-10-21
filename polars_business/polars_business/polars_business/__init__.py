import polars as pl
from polars.type_aliases import IntoExpr
from polars.utils.udfs import _get_shared_lib_location
from datetime import date

lib = _get_shared_lib_location(__file__)

__version__ = "0.1.21"

mapping = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


@pl.api.register_expr_namespace("business")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def advance_n_days(self, n, weekend=("Sat", "Sun"), holidays=None) -> pl.Expr:
        import warnings
        warnings.warn(
            "`.business.advance_n_days` has been renamed to `.bdt.offset_by`, please use that instead.",
            DeprecationWarning,
            stacklevel=3,
            )
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

@pl.api.register_expr_namespace("bdt")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def offset_by(self, by, *, weekend=("Sat", "Sun"), holidays=None) -> pl.Expr:
        if not isinstance(by, pl.Expr):
            by = pl.lit(by)
        negate = 2*by.str.starts_with('-').cast(pl.Int32) - 1
        n = by.str.extract(r'(\d+)bd').cast(pl.Int32) * negate * -1
        by = by.str.replace(r'(\d+bd)', '')

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

        return self._expr._register_plugin(
            lib=lib,
            symbol="advance_n_days",
            is_elementwise=True,
            args=[n],
            kwargs={
                "holidays": holidays,
                "weekend": weekend,
            },
        ).dt.offset_by(by)
