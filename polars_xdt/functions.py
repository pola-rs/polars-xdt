from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl
from polars.plugins import register_plugin_function

from polars_xdt.utils import parse_into_expr

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from collections.abc import Sequence

    from polars import Expr

    from polars_xdt.typing import IntoExprColumn

    Ambiguous: TypeAlias = Literal["earliest", "latest", "raise", "null"]

RollStrategy: TypeAlias = Literal["raise", "forward", "backward"]


PLUGIN_PATH = Path(__file__).parent

mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}


def get_weekmask(weekend: Sequence[str]) -> list[bool]:
    if weekend == ("Sat", "Sun"):
        weekmask = [True, True, True, True, True, False, False]
    else:
        weekmask = [reverse_mapping[i] not in weekend for i in range(1, 8)]
    if sum(weekmask) == 0:
        msg = f"At least one day of the week must be a business day. Got weekend={weekend}"
        raise ValueError(msg)
    return weekmask


def is_workday(
    expr: IntoExprColumn,
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
    weekend_int = [mapping[x] for x in weekend]
    if holidays is not None:
        return ~(
            expr.dt.date().is_in(holidays)
            | expr.dt.weekday().is_in(weekend_int)
        )
    return ~expr.dt.weekday().is_in(weekend_int)


def from_local_datetime(
    expr: IntoExprColumn,
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
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="from_local_datetime",
        is_elementwise=True,
        args=[expr, from_tz],
        kwargs={
            "to_tz": to_tz,
            "ambiguous": ambiguous,
        },
    )


def to_local_datetime(
    expr: IntoExprColumn,
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
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="to_local_datetime",
        is_elementwise=True,
        args=[expr, time_zone],
    )


def format_localized(
    expr: IntoExprColumn,
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
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="format_localized",
        is_elementwise=True,
        args=[expr],
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
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="to_julian_date",
        is_elementwise=True,
        args=[expr],
    )


def ceil(
    expr: IntoExprColumn,
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


def month_delta(
    start_dates: IntoExprColumn,
    end_dates: IntoExprColumn | date,
) -> pl.Expr:
    """
    Calculate the number of months between two Series.

    Parameters
    ----------
    start_dates
        A Series object containing the start dates.
    end_dates
        A Series object containing the end dates.

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
    ...         "start_date": [
    ...             date(2024, 3, 1),
    ...             date(2024, 3, 31),
    ...             date(2022, 2, 28),
    ...             date(2023, 1, 31),
    ...             date(2019, 12, 31),
    ...         ],
    ...         "end_date": [
    ...             date(2023, 2, 28),
    ...             date(2023, 2, 28),
    ...             date(2023, 2, 28),
    ...             date(2023, 1, 31),
    ...             date(2023, 1, 1),
    ...         ],
    ...     },
    ... )
    >>> df.with_columns(
    ...     xdt.month_delta("start_date", "end_date").alias("month_delta")
    ... )
    shape: (5, 3)
    ┌────────────┬────────────┬─────────────┐
    │ start_date ┆ end_date   ┆ month_delta │
    │ ---        ┆ ---        ┆ ---         │
    │ date       ┆ date       ┆ i32         │
    ╞════════════╪════════════╪═════════════╡
    │ 2024-03-01 ┆ 2023-02-28 ┆ -12         │
    │ 2024-03-31 ┆ 2023-02-28 ┆ -13         │
    │ 2022-02-28 ┆ 2023-02-28 ┆ 12          │
    │ 2023-01-31 ┆ 2023-01-31 ┆ 0           │
    │ 2019-12-31 ┆ 2023-01-01 ┆ 36          │
    └────────────┴────────────┴─────────────┘

    """
    start_dates = parse_into_expr(start_dates)
    if not isinstance(end_dates, date):
        end_dates = parse_into_expr(end_dates)

    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="month_delta",
        is_elementwise=True,
        args=[start_dates, end_dates],
    )


def arg_previous_greater(expr: IntoExprColumn) -> pl.Expr:
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
    return register_plugin_function(
        plugin_path=PLUGIN_PATH,
        function_name="arg_previous_greater",
        is_elementwise=False,
        args=[expr],
    )
