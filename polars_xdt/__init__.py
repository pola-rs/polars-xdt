from __future__ import annotations

import polars as pl
from polars.utils.udfs import _get_shared_lib_location
import re
from datetime import date
import sys
from polars.utils._parse_expr_input import parse_as_expression
from polars.utils._wrap import wrap_expr
from polars_xdt.ranges import date_range

from polars.type_aliases import PolarsDataType
from typing import Iterable, Literal, Protocol, Sequence, cast, TYPE_CHECKING

from ._internal import __version__ as __version__

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

RollStrategy: TypeAlias = Literal["raise", "forward", "backward"]


lib = _get_shared_lib_location(__file__)

mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}

if TYPE_CHECKING:
    from polars import Expr
    from polars.type_aliases import Ambiguous


def get_weekmask(weekend: Sequence[str]) -> list[bool]:
    if weekend == ("Sat", "Sun"):
        weekmask = [True, True, True, True, True, False, False]
    else:
        weekmask = [False if reverse_mapping[i] in weekend else True for i in range(1, 8)]
    if sum(weekmask) == 0:
        raise ValueError(
            f"At least one day of the week must be a business day. Got weekend={weekend}"
        )
    return weekmask


@pl.api.register_expr_namespace("xdt")
class ExprXDTNamespace:
    """eXtra stuff for DateTimes."""

    def __init__(self, expr: pl.Expr):
        self._expr = expr

    def offset_by(
        self,
        by: str | pl.Expr,
        *,
        weekend: Sequence[str] = ("Sat", "Sun"),
        holidays: Sequence[date] | None = None,
        roll: RollStrategy = "raise",
    ) -> xdtExpr:
        """Offset this date by a relative time offset.

        Parameters
        ----------
        by
            The offset to apply. This can be a string of the form "nbd" (where n
            is an integer), or a polars expression that evaluates to such a string.
            Additional units are passed to `polars.dt.offset_by`.
        weekend
            The days of the week that are considered weekends. Defaults to ("Sat", "Sun").
        holidays
            The holidays to exclude from the calculation. Defaults to None.
        roll
            How to handle dates that fall on a non-workday.

            - "raise" raise an error (default).
            - "forward" roll forward to the next business day.
            - "backward" roll backward to the previous business day.

        Returns
        -------
        polars.Expr

        Examples
        --------
        >>> import polars as pl
        >>> import polars_xdt  # noqa: F401
        >>> df = pl.DataFrame(
        ...     {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
        ... )
        >>> df.with_columns(
        ...     date_shifted=pl.col("date").xdt.offset_by("1bd"),
        ... )
        shape: (3, 2)
        ┌────────────┬──────────────┐
        │ date       ┆ date_shifted │
        │ ---        ┆ ---          │
        │ date       ┆ date         │
        ╞════════════╪══════════════╡
        │ 2023-04-03 ┆ 2023-04-04   │
        │ 2023-09-01 ┆ 2023-09-04   │
        │ 2024-01-04 ┆ 2024-01-05   │
        └────────────┴──────────────┘

        You can also specify custom weekends and holidays:

        >>> import holidays
        >>> holidays_england = holidays.country_holidays(
        ...     "UK", subdiv="ENG", years=[2023, 2024]
        ... )
        >>> df.with_columns(
        ...     date_shifted=pl.col("date").xdt.offset_by(
        ...         "5bd",
        ...         holidays=holidays_england,
        ...         weekend=["Fri", "Sat"],
        ...         roll="backward",
        ...     ),
        ... )
        shape: (3, 2)
        ┌────────────┬──────────────┐
        │ date       ┆ date_shifted │
        │ ---        ┆ ---          │
        │ date       ┆ date         │
        ╞════════════╪══════════════╡
        │ 2023-04-03 ┆ 2023-04-11   │
        │ 2023-09-01 ┆ 2023-09-07   │
        │ 2024-01-04 ┆ 2024-01-11   │
        └────────────┴──────────────┘

        You can also pass expressions to `by`:

        >>> df = pl.DataFrame(
        ...     {
        ...         "date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)],
        ...         "by": ["1bd", "2bd", "-3bd"],
        ...     }
        ... )
        >>> df.with_columns(date_shifted=pl.col("date").xdt.offset_by(pl.col("by")))
        shape: (3, 3)
        ┌────────────┬──────┬──────────────┐
        │ date       ┆ by   ┆ date_shifted │
        │ ---        ┆ ---  ┆ ---          │
        │ date       ┆ str  ┆ date         │
        ╞════════════╪══════╪══════════════╡
        │ 2023-04-03 ┆ 1bd  ┆ 2023-04-04   │
        │ 2023-09-01 ┆ 2bd  ┆ 2023-09-05   │
        │ 2024-01-04 ┆ -3bd ┆ 2024-01-01   │
        └────────────┴──────┴──────────────┘
        """
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
            n = (by.str.extract(r"^(-?)") + by.str.extract(r"(\d+)bd")).cast(pl.Int32)
            by = by.str.replace(r"(\d+bd)", "")
            fastpath = False

        if not holidays:
            holidays_int = []
        else:
            holidays_int = sorted({(holiday - date(1970, 1, 1)).days for holiday in holidays})
        weekmask = get_weekmask(weekend)

        result = self._expr.register_plugin(
            lib=lib,
            symbol="advance_n_days",
            is_elementwise=True,
            args=[n],
            kwargs={
                "holidays": holidays_int,
                "weekmask": weekmask,
                "roll": roll,
            },
        )
        if fastpath:
            return cast(xdtExpr, result)
        return cast(xdtExpr, result.dt.offset_by(by))

    def sub(
        self,
        end_dates: str | pl.Expr,
        *,
        weekend: Sequence[str] = ("Sat", "Sun"),
        holidays: Sequence[date] | None = None,
    ) -> xdtExpr:
        weekmask = get_weekmask(weekend)
        if not holidays:
            holidays_int = []
        else:
            holidays_int = sorted({(holiday - date(1970, 1, 1)).days for holiday in holidays})
        if isinstance(end_dates, str):
            end_dates = pl.col(end_dates)
        result = self._expr.register_plugin(
            lib=lib,
            symbol="sub",
            is_elementwise=True,
            args=[end_dates],
            kwargs={
                "weekmask": weekmask,
                "holidays": holidays_int,
            },
        )
        return cast(xdtExpr, result)

    def is_workday(
        self,
        *,
        weekend: Sequence[str] = ("Sat", "Sun"),
        holidays: Sequence[date] | None = None,
    ) -> pl.Expr:
        weekmask = get_weekmask(weekend)
        if not holidays:
            holidays_int = []
        else:
            holidays_int = sorted({(holiday - date(1970, 1, 1)).days for holiday in holidays})
        result = self._expr.register_plugin(
            lib=lib,
            symbol="is_workday",
            is_elementwise=True,
            args=[],
            kwargs={
                "weekmask": weekmask,
                "holidays": holidays_int,
            },
        )
        return result

    def from_local_datetime(
        self,
        from_tz: str | Expr,
        to_tz: str,
        ambiguous: Ambiguous = "raise",
    ) -> xdtExpr:
        """Converts from local datetime in given time zone to new timezone.

        Parameters
        ----------
        from_tz
            Current timezone of each datetime
        to_tz
            Timezone to convert to
        ambiguous
            Determine how to deal with ambiguous datetimes:

            - `'raise'` (default): raise
            - `'earliest'`: use the earliest datetime
            - `'latest'`: use the latest datetime

        Returns
        -------
        Expr
            Expression of data type :class:`DateTime`.

        Examples
        --------
        You can go from a localized datetime back to expressing the datetimes
        in a single timezone with `from_local_datetime`.

        >>> from datetime import datetime
        >>> df = pl.DataFrame(
        ...     {
        ...         "local_dt": [
        ...             datetime(2020, 10, 10, 1),
        ...             datetime(2020, 10, 10, 2),
        ...             datetime(2020, 10, 9, 20),
        ...         ],
        ...         "timezone": ["Europe/London", "Africa/Kigali", "America/New_York"],
        ...     }
        ... )
        >>> df.with_columns(
        ...     pl.col("local_dt")
        ...     .xdt.from_local_datetime(pl.col("timezone"), "UTC")
        ...     .alias("date")
        ... )
        shape: (3, 3)
        ┌─────────────────────┬──────────────────┬─────────────────────────┐
        │ local_dt            ┆ timezone         ┆ date                    │
        │ ---                 ┆ ---              ┆ ---                     │
        │ datetime[μs]        ┆ str              ┆ datetime[μs, UTC]       │
        ╞═════════════════════╪══════════════════╪═════════════════════════╡
        │ 2020-10-10 01:00:00 ┆ Europe/London    ┆ 2020-10-10 00:00:00 UTC │
        │ 2020-10-10 02:00:00 ┆ Africa/Kigali    ┆ 2020-10-10 00:00:00 UTC │
        │ 2020-10-09 20:00:00 ┆ America/New_York ┆ 2020-10-10 00:00:00 UTC │
        └─────────────────────┴──────────────────┴─────────────────────────┘
        """
        from_tz = wrap_expr(parse_as_expression(from_tz, str_as_lit=True))
        result = self._expr.register_plugin(
            lib=lib,
            symbol="from_local_datetime",
            is_elementwise=True,
            args=[from_tz],
            kwargs={
                "to_tz": to_tz,
                "ambiguous": ambiguous,
            },
        )
        return cast(xdtExpr, result)

    def to_local_datetime(
        self,
        time_zone: str | Expr,
    ) -> xdtExpr:
        """Convert to local datetime in given time zone.

        Parameters
        ----------
        time_zone
            Time zone to convert to.

        Returns
        -------
        Expr
            Expression of data type :class:`DateTime`.

        Examples
        --------
        You can use `to_local_datetime` to figure out how a tz-aware datetime
        will be expressed as a local datetime.

        >>> from datetime import datetime
        >>> df = pl.DataFrame(
        ...     {
        ...         "date_col": [datetime(2020, 10, 10)] * 3,
        ...         "timezone": ["Europe/London", "Africa/Kigali", "America/New_York"],
        ...     }
        ... ).with_columns(pl.col("date_col").dt.replace_time_zone("UTC"))
        >>> df.with_columns(
        ...     pl.col("date_col")
        ...     .xdt.to_local_datetime(pl.col("timezone"))
        ...     .alias("local_dt")
        ... )
        shape: (3, 3)
        ┌─────────────────────────┬──────────────────┬─────────────────────┐
        │ date_col                ┆ timezone         ┆ local_dt            │
        │ ---                     ┆ ---              ┆ ---                 │
        │ datetime[μs, UTC]       ┆ str              ┆ datetime[μs]        │
        ╞═════════════════════════╪══════════════════╪═════════════════════╡
        │ 2020-10-10 00:00:00 UTC ┆ Europe/London    ┆ 2020-10-10 01:00:00 │
        │ 2020-10-10 00:00:00 UTC ┆ Africa/Kigali    ┆ 2020-10-10 02:00:00 │
        │ 2020-10-10 00:00:00 UTC ┆ America/New_York ┆ 2020-10-09 20:00:00 │
        └─────────────────────────┴──────────────────┴─────────────────────┘
        """
        time_zone = wrap_expr(parse_as_expression(time_zone, str_as_lit=True))
        result = self._expr.register_plugin(
            lib=lib,
            symbol="to_local_datetime",
            is_elementwise=True,
            args=[time_zone],
        )
        return cast(xdtExpr, result)


class xdtExpr(pl.Expr):
    @property
    def xdt(self) -> ExprXDTNamespace:
        return ExprXDTNamespace(self)


class xdtColumn(Protocol):
    def __call__(
        self,
        name: str | PolarsDataType | Iterable[str] | Iterable[PolarsDataType],
        *more_names: str | PolarsDataType,
    ) -> xdtExpr:
        ...

    def __getattr__(self, name: str) -> pl.Expr:
        ...

    @property
    def xdt(self) -> ExprXDTNamespace:
        ...


col = cast(xdtColumn, pl.col)


def workday_count(
    start: str | pl.Expr | date,
    end: str | pl.Expr | date,
    weekend: Sequence[str] = ("Sat", "Sun"),
    holidays: Sequence[date] | None = None,
) -> xdtExpr:
    """Count the number of workdays between two columns of dates.

    Parameters
    ----------
    start
        Start date(s). This can be a string column, a date column, or a single date.
    end
        End date(s). This can be a string column, a date column, or a single date.
    weekend
        The days of the week that are considered weekends. Defaults to ("Sat", "Sun").
    holidays
        The holidays to exclude from the calculation. Defaults to None. This should
        be a list of ``datetime.date`` s.

    Returns
    -------
    polars.Expr

    Examples
    --------
    >>> from datetime import date
    >>> import polars as pl
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "start": [date(2023, 1, 4), date(2023, 5, 1), date(2023, 9, 9)],
    ...         "end": [date(2023, 2, 8), date(2023, 5, 2), date(2023, 12, 30)],
    ...     }
    ... )
    >>> df.with_columns(n_business_days=xdt.workday_count("start", "end"))
    shape: (3, 3)
    ┌────────────┬────────────┬─────────────────┐
    │ start      ┆ end        ┆ n_business_days │
    │ ---        ┆ ---        ┆ ---             │
    │ date       ┆ date       ┆ i32             │
    ╞════════════╪════════════╪═════════════════╡
    │ 2023-01-04 ┆ 2023-02-08 ┆ 25              │
    │ 2023-05-01 ┆ 2023-05-02 ┆ 1               │
    │ 2023-09-09 ┆ 2023-12-30 ┆ 80              │
    └────────────┴────────────┴─────────────────┘
    """
    if isinstance(start, str):
        start = col(start)
    elif not isinstance(start, pl.Expr):
        start = pl.lit(start)
    if isinstance(end, str):
        end = col(end)
    elif not isinstance(end, pl.Expr):
        end = pl.lit(end)

    return end.xdt.sub(start, weekend=weekend, holidays=holidays).alias("workday_count")  # type: ignore[no-any-return, attr-defined]


__all__ = [
    "col",
    "date_range",
    "workday_count",
]
