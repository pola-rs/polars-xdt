from __future__ import annotations

import re
import sys
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal, Sequence

import polars as pl
from polars.utils.udfs import _get_shared_lib_location

from polars_xdt.utils import parse_into_expr

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

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
        weekmask = [reverse_mapping[i] not in weekend for i in range(1, 8)]
    if sum(weekmask) == 0:
        msg = f"At least one day of the week must be a business day. Got weekend={weekend}"
        raise ValueError(msg)
    return weekmask


def offset_by(
    expr: IntoExpr,
    by: IntoExpr,
    *,
    weekend: Sequence[str] = ("Sat", "Sun"),
    holidays: Sequence[date] | None = None,
    roll: RollStrategy = "raise",
) -> pl.Expr:
    """
    Offset this date by a relative time offset.

    Parameters
    ----------
    expr
        Expression to offset by relative time offset.
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
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
    ... )
    >>> df.with_columns(
    ...     date_shifted=xdt.offset_by("date", "1bd"),
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
    ...     date_shifted=xdt.offset_by(
    ...         "date",
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
    ...         "date": [
    ...             date(2023, 4, 3),
    ...             date(2023, 9, 1),
    ...             date(2024, 1, 4),
    ...         ],
    ...         "by": ["1bd", "2bd", "-3bd"],
    ...     }
    ... )
    >>> df.with_columns(date_shifted=xdt.offset_by("date", pl.col("by")))
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
    expr = parse_into_expr(expr)
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
        n = (by.str.extract(r"^(-?)") + by.str.extract(r"(\d+)bd")).cast(
            pl.Int32,
        )
        by = by.str.replace(r"(\d+bd)", "")
        fastpath = False

    if not holidays:
        holidays_int = []
    else:
        holidays_int = sorted(
            {(holiday - date(1970, 1, 1)).days for holiday in holidays},
        )
    weekmask = get_weekmask(weekend)

    result = expr.register_plugin(
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
        return result
    return result.dt.offset_by(by)


def is_workday(
    expr: IntoExpr,
    *,
    weekend: Sequence[str] = ("Sat", "Sun"),
    holidays: Sequence[date] | None = None,
) -> pl.Expr:
    """
    Determine whether a day is a workday.

    Parameters
    ----------
    expr
        Input expression.
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
    ...         "date": [
    ...             date(2023, 1, 4),
    ...             date(2023, 5, 1),
    ...             date(2023, 9, 9),
    ...         ],
    ...     }
    ... )
    >>> df.with_columns(is_workday=xdt.is_workday("date"))
    shape: (3, 2)
    ┌────────────┬────────────┐
    │ date       ┆ is_workday │
    │ ---        ┆ ---        │
    │ date       ┆ bool       │
    ╞════════════╪════════════╡
    │ 2023-01-04 ┆ true       │
    │ 2023-05-01 ┆ true       │
    │ 2023-09-09 ┆ false      │
    └────────────┴────────────┘

    """
    expr = parse_into_expr(expr)
    weekmask = get_weekmask(weekend)
    if not holidays:
        holidays_int = []
    else:
        holidays_int = sorted(
            {(holiday - date(1970, 1, 1)).days for holiday in holidays},
        )
    return expr.register_plugin(
        lib=lib,
        symbol="is_workday",
        is_elementwise=True,
        args=[],
        kwargs={
            "weekmask": weekmask,
            "holidays": holidays_int,
        },
    )


def from_local_datetime(
    expr: IntoExpr,
    from_tz: str | Expr,
    to_tz: str,
    ambiguous: Ambiguous = "raise",
) -> pl.Expr:
    """
    Convert from local datetime in given time zone to new timezone.

    Parameters
    ----------
    expr
        Expression to convert.
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
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "local_dt": [
    ...             datetime(2020, 10, 10, 1),
    ...             datetime(2020, 10, 10, 2),
    ...             datetime(2020, 10, 9, 20),
    ...         ],
    ...         "timezone": [
    ...             "Europe/London",
    ...             "Africa/Kigali",
    ...             "America/New_York",
    ...         ],
    ...     }
    ... )
    >>> df.with_columns(
    ...     xdt.from_local_datetime(
    ...         "local_dt", pl.col("timezone"), "UTC"
    ...     ).alias("date")
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
    expr = parse_into_expr(expr)
    from_tz = parse_into_expr(from_tz, str_as_lit=True)
    return expr.register_plugin(
        lib=lib,
        symbol="from_local_datetime",
        is_elementwise=True,
        args=[from_tz],
        kwargs={
            "to_tz": to_tz,
            "ambiguous": ambiguous,
        },
    )


def to_local_datetime(
    expr: IntoExpr,
    time_zone: str | Expr,
) -> pl.Expr:
    """
    Convert to local datetime in given time zone.

    Parameters
    ----------
    expr
        Expression to convert.
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
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [datetime(2020, 10, 10)] * 3,
    ...         "timezone": [
    ...             "Europe/London",
    ...             "Africa/Kigali",
    ...             "America/New_York",
    ...         ],
    ...     }
    ... ).with_columns(pl.col("date_col").dt.replace_time_zone("UTC"))
    >>> df.with_columns(
    ...     xdt.to_local_datetime("date_col", pl.col("timezone")).alias(
    ...         "local_dt"
    ...     )
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
    expr = parse_into_expr(expr)
    time_zone = parse_into_expr(time_zone, str_as_lit=True)
    return expr.register_plugin(
        lib=lib,
        symbol="to_local_datetime",
        is_elementwise=True,
        args=[time_zone],
    )


def format_localized(
    expr: IntoExpr,
    format: str,  # noqa: A002
    locale: str = "uk_UA",
) -> pl.Expr:
    """
    Convert to local datetime in given time zone.

    Parameters
    ----------
    expr
        Expression to format.
    format
        Format string, see https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        for what's available.
    locale
        Locale to use for formatting. Defaults to "uk_UA", because that's what the OP
        requested https://github.com/pola-rs/polars/issues/12341.

    Returns
    -------
    Expr
        Expression of data type :class:`Utf8`.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [datetime(2024, 8, 24), datetime(2024, 10, 1)],
    ...     }
    ... )
    >>> df.with_columns(
    ...     result=xdt.format_localized(
    ...         "date_col", format="%A, %d %B %Y", locale="uk_UA"
    ...     )
    ... )
    shape: (2, 2)
    ┌─────────────────────┬──────────────────────────┐
    │ date_col            ┆ result                   │
    │ ---                 ┆ ---                      │
    │ datetime[μs]        ┆ str                      │
    ╞═════════════════════╪══════════════════════════╡
    │ 2024-08-24 00:00:00 ┆ субота, 24 серпня 2024   │
    │ 2024-10-01 00:00:00 ┆ вівторок, 01 жовтня 2024 │
    └─────────────────────┴──────────────────────────┘

    """
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="format_localized",
        is_elementwise=True,
        args=[],
        kwargs={"format": format, "locale": locale},
    )


def to_julian_date(expr: str | pl.Expr) -> pl.Expr:
    """
    Return the Julian date corresponding to given datetimes.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [
    ...             datetime(2013, 1, 1, 0, 30),
    ...             datetime(2024, 1, 7, 13, 18, 51),
    ...         ],
    ...     }
    ... )
    >>> with pl.Config(float_precision=10) as cfg:
    ...     df.with_columns(julian_date=xdt.to_julian_date("date_col"))
    shape: (2, 2)
    ┌─────────────────────┬────────────────────┐
    │ date_col            ┆ julian_date        │
    │ ---                 ┆ ---                │
    │ datetime[μs]        ┆ f64                │
    ╞═════════════════════╪════════════════════╡
    │ 2013-01-01 00:30:00 ┆ 2456293.5208333335 │
    │ 2024-01-07 13:18:51 ┆ 2460317.0547569445 │
    └─────────────────────┴────────────────────┘

    """
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="to_julian_date",
        is_elementwise=True,
        args=[],
    )


def ceil(
    expr: IntoExpr,
    every: str | pl.Expr,
) -> pl.Expr:
    """
    Find "ceiling" of datetime.

    Parameters
    ----------
    expr
        Expression to take "ceiling" of.
    every
        Duration string, created with the
        the following string language:

        - 1ns   (1 nanosecond)
        - 1us   (1 microsecond)
        - 1ms   (1 millisecond)
        - 1s    (1 second)
        - 1m    (1 minute)
        - 1h    (1 hour)
        - 1d    (1 calendar day)
        - 1w    (1 calendar week)
        - 1mo   (1 calendar month)
        - 1q    (1 calendar quarter)
        - 1y    (1 calendar year)

        These strings can be combined:

        - 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds

        By "calendar day", we mean the corresponding time on the next day (which may
        not be 24 hours, due to daylight savings). Similarly for "calendar week",
        "calendar month", "calendar quarter", and "calendar year".

    Returns
    -------
    Expr
        Expression of data type :class:`Utf8`.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "date_col": [datetime(2024, 8, 24), datetime(2024, 10, 1)],
    ...     }
    ... )
    >>> df.with_columns(result=xdt.ceil("date_col", "1mo"))
    shape: (2, 2)
    ┌─────────────────────┬─────────────────────┐
    │ date_col            ┆ result              │
    │ ---                 ┆ ---                 │
    │ datetime[μs]        ┆ datetime[μs]        │
    ╞═════════════════════╪═════════════════════╡
    │ 2024-08-24 00:00:00 ┆ 2024-09-01 00:00:00 │
    │ 2024-10-01 00:00:00 ┆ 2024-10-01 00:00:00 │
    └─────────────────────┴─────────────────────┘

    """
    expr = parse_into_expr(expr)
    truncated = expr.dt.truncate(every)
    return (
        pl.when(expr == truncated)
        .then(expr)
        .otherwise(truncated.dt.offset_by(every))
    )


def day_name(expr: str | pl.Expr, locale: str | None = None) -> pl.Expr:
    """
    Return day name, in specified locale (if specified).

    Returns
    -------
    Expr
        Expression of data type :class:`Utf8`.

    See Also
    --------
    format_localized : format according to locale.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "ts": [datetime(2020, 10, 25), datetime(2020, 10, 26)],
    ...     }
    ... )
    >>> df.with_columns(
    ...     english_day_name=xdt.day_name("ts"),
    ...     french_day_name=xdt.day_name("ts", locale="fr_FR"),
    ...     ukrainian_day_name=xdt.day_name("ts", locale="uk_UA"),
    ... )
    shape: (2, 4)
    ┌─────────────────────┬──────────────────┬─────────────────┬────────────────────┐
    │ ts                  ┆ english_day_name ┆ french_day_name ┆ ukrainian_day_name │
    │ ---                 ┆ ---              ┆ ---             ┆ ---                │
    │ datetime[μs]        ┆ str              ┆ str             ┆ str                │
    ╞═════════════════════╪══════════════════╪═════════════════╪════════════════════╡
    │ 2020-10-25 00:00:00 ┆ Sunday           ┆ dimanche        ┆ неділя             │
    │ 2020-10-26 00:00:00 ┆ Monday           ┆ lundi           ┆ понеділок          │
    └─────────────────────┴──────────────────┴─────────────────┴────────────────────┘

    """
    expr = parse_into_expr(expr)
    if locale is None:
        result = expr.dt.to_string("%A")
    else:
        result = format_localized(expr, "%A", locale=locale)  # type: ignore[attr-defined]
    return result


def month_name(expr: str | pl.Expr, locale: str | None = None) -> pl.Expr:
    """
    Return month name, in specified locale (if specified).

    Returns
    -------
    Expr
        Expression of data type :class:`Utf8`.

    See Also
    --------
    format_localized : format according to locale.

    Examples
    --------
    >>> from datetime import datetime
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame(
    ...     {
    ...         "ts": [datetime(2020, 10, 25), datetime(2020, 11, 26)],
    ...     }
    ... )
    >>> df.with_columns(
    ...     english_month_name=xdt.month_name("ts"),
    ...     french_month_name=xdt.month_name("ts", locale="fr_FR"),
    ...     ukrainian_month_name=xdt.month_name("ts", locale="uk_UA"),
    ... )
    shape: (2, 4)
    ┌─────────────────────┬────────────────────┬───────────────────┬──────────────────────┐
    │ ts                  ┆ english_month_name ┆ french_month_name ┆ ukrainian_month_name │
    │ ---                 ┆ ---                ┆ ---               ┆ ---                  │
    │ datetime[μs]        ┆ str                ┆ str               ┆ str                  │
    ╞═════════════════════╪════════════════════╪═══════════════════╪══════════════════════╡
    │ 2020-10-25 00:00:00 ┆ October            ┆ octobre           ┆ жовтня               │
    │ 2020-11-26 00:00:00 ┆ November           ┆ novembre          ┆ листопада            │
    └─────────────────────┴────────────────────┴───────────────────┴──────────────────────┘

    """
    expr = parse_into_expr(expr)
    if locale is None:
        result = expr.dt.to_string("%B")
    else:
        result = format_localized(expr, "%B", locale=locale)
    return result


def workday_count(
    start_dates: IntoExpr,
    end_dates: IntoExpr,
    weekend: Sequence[str] = ("Sat", "Sun"),
    holidays: Sequence[date] | None = None,
) -> pl.Expr:
    """
    Count the number of workdays between two columns of dates.

    Parameters
    ----------
    start_dates
        Start date(s). This can be a string column, a date column, or a single date.
    end_dates
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
    start_dates = parse_into_expr(start_dates)
    end_dates = parse_into_expr(end_dates)
    weekmask = get_weekmask(weekend)
    if not holidays:
        holidays_int = []
    else:
        holidays_int = sorted(
            {(holiday - date(1970, 1, 1)).days for holiday in holidays},
        )
    return start_dates.register_plugin(
        lib=lib,
        symbol="workday_count",
        is_elementwise=True,
        args=[end_dates],
        kwargs={
            "weekmask": weekmask,
            "holidays": holidays_int,
        },
    )


def arg_previous_greater(expr: IntoExpr) -> pl.Expr:
    """
    Find the row count of the previous value greater than the current one.

    Parameters
    ----------
    expr
        Expression.

    Returns
    -------
    Expr
        UInt64 or UInt32 type, depending on the platform.

    Examples
    --------
    >>> import polars as pl
    >>> import polars_xdt as xdt
    >>> df = pl.DataFrame({"value": [1, 9, 6, 7, 3]})
    >>> df.with_columns(result=xdt.arg_previous_greater("value"))
    shape: (5, 2)
    ┌───────┬────────┐
    │ value ┆ result │
    │ ---   ┆ ---    │
    │ i64   ┆ u32    │
    ╞═══════╪════════╡
    │ 1     ┆ null   │
    │ 9     ┆ 1      │
    │ 6     ┆ 1      │
    │ 7     ┆ 1      │
    │ 3     ┆ 3      │
    └───────┴────────┘

    This can be useful when working with time series. For example,
    if you a dataset like this:

    >>> df = pl.DataFrame(
    ...     {
    ...         "date": [
    ...             "2024-02-01",
    ...             "2024-02-02",
    ...             "2024-02-03",
    ...             "2024-02-04",
    ...             "2024-02-05",
    ...             "2024-02-06",
    ...             "2024-02-07",
    ...             "2024-02-08",
    ...             "2024-02-09",
    ...             "2024-02-10",
    ...         ],
    ...         "group": ["A", "A", "A", "A", "A", "B", "B", "B", "B", "B"],
    ...         "value": [1, 9, None, 7, 3, 2, 4, 5, 1, 9],
    ...     }
    ... )
    >>> df = df.with_columns(pl.col("date").str.to_date())

    and want find out, for each day and each item, how many days it's
    been since `'value'` was higher than it currently is, you could do

    >>> df.with_columns(
    ...     result=(
    ...         (
    ...             pl.col("date")
    ...             - pl.col("date")
    ...             .gather(xdt.arg_previous_greater("value"))
    ...             .over("group")
    ...         ).dt.total_days()
    ...     ),
    ... )
    shape: (10, 4)
    ┌────────────┬───────┬───────┬────────┐
    │ date       ┆ group ┆ value ┆ result │
    │ ---        ┆ ---   ┆ ---   ┆ ---    │
    │ date       ┆ str   ┆ i64   ┆ i64    │
    ╞════════════╪═══════╪═══════╪════════╡
    │ 2024-02-01 ┆ A     ┆ 1     ┆ null   │
    │ 2024-02-02 ┆ A     ┆ 9     ┆ 0      │
    │ 2024-02-03 ┆ A     ┆ null  ┆ null   │
    │ 2024-02-04 ┆ A     ┆ 7     ┆ 2      │
    │ 2024-02-05 ┆ A     ┆ 3     ┆ 1      │
    │ 2024-02-06 ┆ B     ┆ 2     ┆ null   │
    │ 2024-02-07 ┆ B     ┆ 4     ┆ 0      │
    │ 2024-02-08 ┆ B     ┆ 5     ┆ 0      │
    │ 2024-02-09 ┆ B     ┆ 1     ┆ 1      │
    │ 2024-02-10 ┆ B     ┆ 9     ┆ 0      │
    └────────────┴───────┴───────┴────────┘

    """
    expr = parse_into_expr(expr)
    return expr.register_plugin(
        lib=lib,
        symbol="arg_previous_greater",
        is_elementwise=False,
    )


def ewma_by_time(
    values: IntoExpr,
    *,
    times: IntoExpr,
    halflife: timedelta,
    adjust: bool = True,
) -> pl.Expr:
    r"""
    Calculate time-based exponentially weighted moving average.

    Given observations :math:`x_1, x_2, \ldots, x_n` at times
    :math:`t_1, t_2, \ldots, t_n`, the **unadjusted** EWMA is calculated as

        .. math::

            y_0 &= x_0

            \alpha_i &= \exp(-\lambda(t_i - t_{i-1}))

            y_i &= \alpha_i x_i + (1 - \alpha_i) y_{i-1}; \quad i > 0

    where :math:`\lambda` equals :math:`\ln(2) / \text{halflife}`.

    The **adjusted** version is

        .. math::

            y_0 &= x_0

            \alpha_i &= (\alpha_{i-1} + 1) * \exp(-\lambda(t_i - t_{i-1}))

            y_i &= (x_i + \alpha_i y_{i-1}) / (1. + \alpha_i);

    Parameters
    ----------
    values
        Values to calculate EWMA for. Should be signed numeric.
    times
        Times corresponding to `values`. Should be ``DateTime`` or ``Date``.
    halflife
        Unit over which observation decays to half its value.
    adjust
        Whether to adjust the result to account for the bias towards the
        initial value. Defaults to True.

    Returns
    -------
    pl.Expr
        Float64

    Examples
    --------
    >>> import polars as pl
    >>> import polars_xdt as xdt
    >>> from datetime import date, timedelta
    >>> df = pl.DataFrame(
    ...     {
    ...         "values": [0, 1, 2, None, 4],
    ...         "times": [
    ...             date(2020, 1, 1),
    ...             date(2020, 1, 3),
    ...             date(2020, 1, 10),
    ...             date(2020, 1, 15),
    ...             date(2020, 1, 17),
    ...         ],
    ...     }
    ... )
    >>> df.with_columns(
    ...     ewma=xdt.ewma_by_time(
    ...         "values", times="times", halflife=timedelta(days=4)
    ...     ),
    ... )
    shape: (5, 3)
    ┌────────┬────────────┬──────────┐
    │ values ┆ times      ┆ ewma     │
    │ ---    ┆ ---        ┆ ---      │
    │ i64    ┆ date       ┆ f64      │
    ╞════════╪════════════╪══════════╡
    │ 0      ┆ 2020-01-01 ┆ 0.0      │
    │ 1      ┆ 2020-01-03 ┆ 0.585786 │
    │ 2      ┆ 2020-01-10 ┆ 1.523889 │
    │ null   ┆ 2020-01-15 ┆ null     │
    │ 4      ┆ 2020-01-17 ┆ 3.233686 │
    └────────┴────────────┴──────────┘

    """
    times = parse_into_expr(times)
    halflife_us = (
        int(halflife.total_seconds()) * 1_000_000 + halflife.microseconds
    )
    return times.register_plugin(
        lib=lib,
        symbol="ewma_by_time",
        is_elementwise=False,
        args=[values],
        kwargs={"halflife": halflife_us, "adjust": adjust},
    )
