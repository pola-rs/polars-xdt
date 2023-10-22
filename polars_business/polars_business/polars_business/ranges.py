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
    Utility function for filtering out weekends and holidays from a date range.
    """
    if weekend == ("Sat", "Sun"):
        weekend_int = [6, 7]
    else:
        weekend_int = sorted({mapping[name] for name in weekend})
    if holidays is None:
        holidays = []

    if not re.match(r"^-?\d+bd$", interval):
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


@overload
def datetime_range(
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
def datetime_range(
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
def datetime_range(
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


def datetime_range(
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
    Utility function for filtering out weekends and holidays from a date range.
    """
    if weekend == ("Sat", "Sun"):
        weekend_int = [6, 7]
    else:
        weekend_int = sorted({mapping[name] for name in weekend})
    if holidays is None:
        holidays = []

    if not re.match(r"^-?\d+bd$", interval):
        raise ValueError(
            "Only intervals of the form 'nbd' (where n is an integer) are supported."
        )
    interval = interval.replace("bd", "d")

    expr = pl.datetime_range(
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
