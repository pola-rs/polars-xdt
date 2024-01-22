import pytest

from datetime import datetime, date

import polars as pl
from polars.type_aliases import TimeUnit
import polars_xdt as xdt


@pytest.mark.parametrize(
    ("time_unit", "expected_us", "expected_ukr"),
    [
        (
            "ms",
            "Wednesday, 01 January 2020 00:00:00.123",
            "середа, 01 січня 2020 00:00:00,123",
        ),
        (
            "us",
            "Wednesday, 01 January 2020 00:00:00.123456",
            "середа, 01 січня 2020 00:00:00,123456",
        ),
        (
            "ns",
            "Wednesday, 01 January 2020 00:00:00.123456789",
            "середа, 01 січня 2020 00:00:00,123456789",
        ),
    ],
)
def test_format_localized_datetime(
    time_unit: TimeUnit, expected_us: str, expected_ukr: str
) -> None:
    df = pl.DataFrame(
        {
            "date_col": ["2020-01-01T00:00:00.123456789"],
        },
    ).select(pl.col("date_col").str.to_datetime(time_unit=time_unit))
    result = df.select(
        result=xdt.format_localized(
            "date_col", "%A, %d %B %Y %H:%M:%S%.f", "en_US"
        )
    )["result"]
    assert result[0] == expected_us
    result = df.select(
        result=xdt.format_localized(
            "date_col", "%A, %d %B %Y %H:%M:%S%.f", "uk_UA"
        )
    )["result"]
    assert result[0] == expected_ukr


def test_format_localized_date() -> None:
    df = pl.DataFrame(
        {
            "date_col": [date(2024, 8, 24), date(2024, 10, 1)],
        },
    )
    result = df.select(
        result=xdt.format_localized("date_col", "%A, %d %B %Y", "en_US")
    )["result"]
    assert result[0] == "Saturday, 24 August 2024"
    assert result[1] == "Tuesday, 01 October 2024"
    result = df.select(
        result=xdt.format_localized("date_col", "%A, %d %B %Y", "uk_UA")
    )["result"]
    assert result[0] == "субота, 24 серпня 2024"
    assert result[1] == "вівторок, 01 жовтня 2024"


def test_tz_aware() -> None:
    df = pl.DataFrame(
        {
            "date_col": [datetime(2024, 8, 24), datetime(2024, 10, 1)],
        },
        schema={"date_col": pl.Datetime("ns", "Europe/London")},
    )
    result = df.select(
        result=xdt.format_localized("date_col", "%A, %d %B %Y %z", "uk_UA")
    )
    assert result["result"][0] == "субота, 24 серпня 2024 +0100"
    assert result["result"][1] == "вівторок, 01 жовтня 2024 +0100"


@pytest.mark.parametrize("time_unit", ["ms", "us", "ns"])
def test_pre_epoch(time_unit: TimeUnit) -> None:
    df = pl.DataFrame(
        {
            "date_col": [datetime(1960, 1, 1, 0, 0, 0, 1)],
        },
        schema={"date_col": pl.Datetime(time_unit, "Europe/London")},
    )
    result = df.select(
        result=xdt.format_localized("date_col", "%A, %d %B %Y %z", "en_US")
    )
    assert result["result"][0] == "Friday, 01 January 1960 +0000"
