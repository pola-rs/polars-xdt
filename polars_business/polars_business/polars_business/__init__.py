import polars as pl
from polars.utils.udfs import _get_shared_lib_location
import re
from datetime import date

from polars_business.ranges import date_range, datetime_range

from polars.type_aliases import PolarsDataType
from typing import Sequence, cast, Iterable, Protocol

lib = _get_shared_lib_location(__file__)

__version__ = "0.2.1"

mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}


def get_weekmask(weekend: Sequence[str]) -> list[bool]:
    if weekend == ("Sat", "Sun"):
        weekmask = [True, True, True, True, True, False, False]
    else:
        weekmask = [
            False if reverse_mapping[i] in weekend else True for i in range(1, 8)
        ]
    if sum(weekmask) == 0:
        raise ValueError(
            f"At least one day of the week must be a business day. Got weekend={weekend}"
        )
    return weekmask


@pl.api.register_expr_namespace("business")
class BusinessDayTools:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def advance_n_days(self, n, weekend=("Sat", "Sun"), holidays=None) -> pl.Expr:  # type: ignore
        import warnings

        warnings.warn(
            "`.business.advance_n_days` has been renamed to `.bdt.offset_by`, please use that instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if not holidays:
            holidays = []
        else:
            holidays = sorted(
                {(holiday - date(1970, 1, 1)).days for holiday in holidays}
            )
        weekmask = get_weekmask(weekend)

        return self._expr._register_plugin(
            lib=lib,
            symbol="advance_n_days",
            is_elementwise=True,
            args=[n],
            kwargs={
                "holidays": holidays,
                "weekmask": weekmask,
            },
        )


@pl.api.register_expr_namespace("bdt")
class ExprBusinessDateTimeNamespace:
    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def offset_by(
        self,
        by: str | pl.Expr,
        *,
        weekend: Sequence[str] = ("Sat", "Sun"),
        holidays: Sequence[date] | None = None,
    ) -> pl.Expr:
        if (
            isinstance(by, str)
            and (match := re.search(r"(\d+bd)", by)) is not None
            and (len(match.group(1)) == len(by))
        ):
            # Fast path - do we have a business day offset, and nothing else?
            n: int | pl.Expr = int(by[:-2])
            fastpath = True
        else:
            if not isinstance(by, pl.Expr):
                by = pl.lit(by)
            negate = 2 * by.str.starts_with("-").cast(pl.Int32) - 1
            n = by.str.extract(r"(\d+)bd").cast(pl.Int32) * negate * -1
            by = by.str.replace(r"(\d+bd)", "")
            fastpath = False

        if not holidays:
            holidays_int = []
        else:
            holidays_int = sorted(
                {(holiday - date(1970, 1, 1)).days for holiday in holidays}
            )
        weekmask = get_weekmask(weekend)

        result = self._expr._register_plugin(
            lib=lib,
            symbol="advance_n_days",
            is_elementwise=True,
            args=[n],
            kwargs={
                "holidays": holidays_int,
                "weekmask": weekmask,
            },
        )
        if fastpath:
            return result
        return result.dt.offset_by(by)

    def sub(
        self,
        end_dates: str | pl.Expr,
        *,
        weekend: Sequence[str] = ("Sat", "Sun"),
        holidays: Sequence[date] | None = None,
    ) -> pl.Expr:
        weekmask = get_weekmask(weekend)
        if not holidays:
            holidays_int = []
        else:
            holidays_int = sorted(
                {
                    (holiday - date(1970, 1, 1)).days
                    for holiday in holidays
                    if holiday.strftime("%a") not in weekend
                }
            )
        if isinstance(end_dates, str):
            end_dates = pl.col(end_dates)
        result = self._expr._register_plugin(
            lib=lib,
            symbol="sub",
            is_elementwise=True,
            args=[end_dates],
            kwargs={
                "weekmask": weekmask,
                "holidays": holidays_int,
            },
        )
        return result


class BExpr(pl.Expr):
    @property
    def bdt(self) -> ExprBusinessDateTimeNamespace:
        return ExprBusinessDateTimeNamespace(self)


class BColumn(Protocol):
    def __call__(
        self,
        name: str | PolarsDataType | Iterable[str] | Iterable[PolarsDataType],
        *more_names: str | PolarsDataType,
    ) -> BExpr:
        ...

    def __getattr__(self, name: str) -> pl.Expr:
        ...

    @property
    def bdt(self) -> ExprBusinessDateTimeNamespace:
        ...


col = cast(BColumn, pl.col)


__all__ = [
    "col",
    "date_range",
    "datetime_range",
]
