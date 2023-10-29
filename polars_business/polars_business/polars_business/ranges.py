from typing import overload
import re

from datetime import datetime, date, timedelta
from typing import Literal, Sequence
import polars as pl
from polars.type_aliases import IntoExprColumn, ClosedInterval, TimeUnit

mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}


@overload
def date_range(
    start: date | datetime | IntoExprColumn,
    end: date | datetime | IntoExprColumn,
    interval: str | timedelta = "1d",
    *,
    closed: ClosedInterval = ...,
    time_unit: TimeUnit | None = ...,
    time_zone: str | None = ...,
    eager: Literal[False] = ...,
    weekend: Sequence[str] = ...,
    holidays: Sequence[date] | None = ...,
) -> pl.Expr:
    ...


@overload
def date_range(
    start: date | datetime | IntoExprColumn,
    end: date | datetime | IntoExprColumn,
    interval: str | timedelta = "1d",
    *,
    closed: ClosedInterval = ...,
    time_unit: TimeUnit | None = ...,
    time_zone: str | None = ...,
    eager: Literal[True],
    weekend: Sequence[str] = ...,
    holidays: Sequence[date] | None = ...,
) -> pl.Series:
    ...


@overload
def date_range(
    start: date | datetime | IntoExprColumn,
    end: date | datetime | IntoExprColumn,
    interval: str | timedelta = "1d",
    *,
    closed: ClosedInterval = ...,
    time_unit: TimeUnit | None = ...,
    time_zone: str | None = ...,
    eager: bool = ...,
    weekend: Sequence[str] = ...,
    holidays: Sequence[date] | None = ...,
) -> pl.Series | pl.Expr:
    ...


def date_range(
    start: date | datetime | IntoExprColumn,
    end: date | datetime | IntoExprColumn,
    interval: str | timedelta = "1bd",
    *,
    closed: ClosedInterval = "both",
    time_unit: TimeUnit | None = None,
    time_zone: str | None = None,
    eager: bool = False,
    weekend: Sequence[str] = ("Sat", "Sun"),
    holidays: Sequence[date] | None = None,
) -> pl.Series | pl.Expr:
    """
    Create a range of dates with a given interval and filter out weekends and holidays.

    Parameters
    ----------
    start
        Lower bound of the date range.
    end
        Upper bound of the date range.
    interval
        Interval of the range periods, specified as a Python ``timedelta`` object
        or using the Polars duration string language (see "Notes" section below).

        To create a month-end date series, combine with :meth:`Expr.dt.month_end` (see
        "Examples" section below).
    closed : {'both', 'left', 'right', 'none'}
        Define which sides of the range are closed (inclusive).
    time_unit : {None, 'ns', 'us', 'ms'}
        Time unit of the resulting ``Datetime`` data type.
        Only takes effect if the output column is of type ``Datetime``.
    time_zone
        Time zone of the resulting ``Datetime`` data type.
        Only takes effect if the output column is of type ``Datetime``.
    eager
        Evaluate immediately and return a ``Series``.
        If set to ``False`` (default), return an expression instead.
    weekend
        The days of the week that are considered weekends. Defaults to ("Sat", "Sun").
    holidays
        The holidays to exclude from the calculation. Defaults to None. This should
        be a list of ``datetime.date`` s.

    Returns
    -------
    Expr or Series
        Column of data type :class:`Date`.

    Examples
    --------
    >>> from datetime import date
    >>> import polars as pl
    >>> import polars_business as plb
    >>> pl.DataFrame(
    ...     {
    ...         "date": plb.date_range(
    ...             date(2023, 1, 1), date(2023, 1, 10), "1bd", eager=True
    ...         ),
    ...     }
    ... )
    shape: (7, 1)
    ┌────────────┐
    │ date       │
    │ ---        │
    │ date       │
    ╞════════════╡
    │ 2023-01-02 │
    │ 2023-01-03 │
    │ 2023-01-04 │
    │ 2023-01-05 │
    │ 2023-01-06 │
    │ 2023-01-09 │
    │ 2023-01-10 │
    └────────────┘
    """
    if weekend == ("Sat", "Sun"):
        weekend_int = [6, 7]
    else:
        weekend_int = sorted({mapping[name] for name in weekend})
    if holidays is None:
        holidays = []

    if not (isinstance(interval, str) and re.match(r"^-?\d+bd$", interval)):
        raise ValueError(
            "Only intervals of the form 'nbd' (where n is an integer) are supported."
        )
    interval = interval.replace("bd", "d")

    expr = pl.date_range(
        start,
        end,
        interval,
        closed=closed,
        time_unit=time_unit,
        time_zone=time_zone,
        eager=False,
    )
    expr = expr.filter(~expr.dt.date().is_in(holidays))
    expr = expr.filter(~expr.dt.weekday().is_in(weekend_int))
    if eager:
        df = pl.select(expr)
        return df[df.columns[0]]
    return expr
