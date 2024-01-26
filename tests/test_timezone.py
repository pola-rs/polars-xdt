from __future__ import annotations
import pytest
from datetime import datetime, timezone

from polars.testing import (
    assert_frame_equal,
)
import polars as pl
import polars_xdt as xdt


@pytest.mark.parametrize(
    ("date", "timezone", "local_date"),
    [
        (
            datetime(2020, 10, 10, tzinfo=timezone.utc),
            "Europe/London",
            datetime(2020, 10, 10, 1, 0),
        ),
        (
            datetime(2020, 10, 15, tzinfo=timezone.utc),
            "Africa/Kigali",
            datetime(2020, 10, 15, 2, 0),
        ),
        (
            datetime(2020, 10, 15, tzinfo=timezone.utc),
            "America/New_York",
            datetime(2020, 10, 14, 20, 0),
        ),
    ],
)
def test_convert_tz_to_local_datetime(
    date: datetime, timezone: str, local_date: datetime
) -> None:
    df = pl.DataFrame({"date": [date], "timezone": [timezone]}).with_columns(
        pl.col("date").dt.convert_time_zone("Europe/London")
    )

    expected = df.with_columns(pl.lit(local_date).alias("local_dt"))

    result = df.with_columns(
        xdt.to_local_datetime("date", pl.col("timezone")).alias("local_dt")
    )

    assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    ("local_date", "timezone", "date"),
    [
        (
            datetime(2020, 10, 10, 1, 0),
            "Europe/London",
            datetime(2020, 10, 10, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 10, 15, 2, 0),
            "Africa/Kigali",
            datetime(2020, 10, 15, tzinfo=timezone.utc),
        ),
        (
            datetime(2020, 10, 14, 20, 0),
            "America/New_York",
            datetime(2020, 10, 15, tzinfo=timezone.utc),
        ),
    ],
)
def test_convert_tz_from_local_datetime(
    local_date: datetime, timezone: str, date: datetime
) -> None:
    df = pl.DataFrame({"local_date": [local_date], "timezone": [timezone]})

    expected = df.with_columns(
        pl.lit(date).alias("date").dt.convert_time_zone("Europe/London")
    )

    result = df.with_columns(
        xdt.from_local_datetime(
            "local_date", pl.col("timezone"), "Europe/London"
        ).alias("date")
    )

    assert_frame_equal(result, expected)


def test_convert_tz_from_local_datetime_literal() -> None:
    df = pl.DataFrame({"local_date": [datetime(2020, 10, 14, 20, 0)]})

    expected = df.with_columns(
        pl.lit(datetime(2020, 10, 15, tzinfo=timezone.utc))
        .alias("date")
        .dt.convert_time_zone("Europe/London")
    )

    result = df.with_columns(
        xdt.from_local_datetime(
            "local_date", "America/New_York", "Europe/London"
        ).alias("date")
    )
    assert_frame_equal(result, expected)


def test_convert_tz_to_local_datetime_literal() -> None:
    df = pl.DataFrame(
        {"date": [datetime(2020, 10, 15, tzinfo=timezone.utc)]}
    ).with_columns(pl.col("date").dt.convert_time_zone("Europe/London"))

    expected = df.with_columns(
        pl.lit(datetime(2020, 10, 14, 20, 0)).alias("local_dt")
    )

    result = df.with_columns(
        xdt.to_local_datetime("date", "America/New_York").alias("local_dt")
    )

    assert_frame_equal(result, expected)


def test_convert_tz_to_local_datetime_schema() -> None:
    df = pl.LazyFrame({"date": [datetime(2020, 10, 15, tzinfo=timezone.utc)]})
    result = df.with_columns(
        xdt.from_local_datetime("date", "America/New_York", "Asia/Kathmandu")
    ).schema["date"]
    assert result == pl.Datetime("us", "Asia/Kathmandu")
    result = (
        df.with_columns(
            xdt.from_local_datetime(
                "date", "America/New_York", "Asia/Kathmandu"
            )
        )
        .collect()
        .schema["date"]
    )
    assert result == pl.Datetime("us", "Asia/Kathmandu")
