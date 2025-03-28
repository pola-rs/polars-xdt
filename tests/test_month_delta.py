from datetime import date

import polars as pl
from dateutil.relativedelta import relativedelta
from hypothesis import example, given, settings
from hypothesis import strategies as st
from polars.testing import assert_frame_equal

import polars_xdt as xdt


@given(
    start_date=st.dates(
        min_value=date(1924, 1, 1), max_value=date(2024, 12, 31)
    ),
    end_date=st.dates(min_value=date(1960, 1, 1), max_value=date(2024, 12, 31)),
)
@example(start_date=date(2022, 2, 28), end_date=date(2024, 2, 29))  # Leap year
@example(start_date=date(2024, 1, 1), end_date=date(2024, 1, 31))  # Same month
@example(start_date=date(1973, 1, 1), end_date=date(1973, 1, 1))  # Same date
@example(start_date=date(2019, 12, 31), end_date=date(2020, 1, 1))  # Border
@example(start_date=date(2018, 12, 1), end_date=date(2020, 1, 1))  # End of year
@example(start_date=date(2022, 12, 1), end_date=date(2020, 1, 1))  # Negative
@example(start_date=date(2000, 3, 29), end_date=date(2003, 1, 28))
@settings(max_examples=500)
def test_month_delta_hypothesis(start_date: date, end_date: date) -> None:
    df = pl.DataFrame(
        {
            "start_date": [start_date],
            "end_date": [end_date],
        }
    )
    result = df.select(result=xdt.month_delta("start_date", "end_date"))[
        "result"
    ].item()

    expected = 0
    if start_date <= end_date:
        while start_date + relativedelta(months=expected + 1) <= end_date:
            expected += 1
    else:
        while start_date + relativedelta(months=expected - 1) >= end_date:
            expected -= 1
    assert result == expected


def test_month_delta_broadcasting() -> None:
    df = pl.DataFrame(
        {
            "start_date": [
                date(2024, 3, 1),
                date(2024, 3, 31),
                date(2022, 2, 28),
                date(2023, 1, 31),
                date(2019, 12, 31),
            ],
        },
    )
    result = df.with_columns(
        xdt.month_delta("start_date", date(2023, 2, 28)).alias("month_delta")
    )
    expected = pl.DataFrame(
        {
            "start_date": [
                date(2024, 3, 1),
                date(2024, 3, 31),
                date(2022, 2, 28),
                date(2023, 1, 31),
                date(2019, 12, 31),
            ],
            "month_delta": [-12, -13, 12, 1, 38],
        },
        schema_overrides={"month_delta": pl.Int32},
    )
    assert_frame_equal(result, expected)


def test_month_delta_broadcasting_all_nulls() -> None:
    df = pl.DataFrame(
        {
            "start_date": [
                date(2024, 3, 1),
                date(2024, 3, 31),
                date(2022, 2, 28),
                date(2023, 1, 31),
                date(2019, 12, 31),
            ],
        },
    )
    result = df.with_columns(
        xdt.month_delta("start_date", pl.Series([None], dtype=pl.Date)).alias(
            "month_delta"
        )
    )
    expected = pl.DataFrame(
        {
            "start_date": [
                date(2024, 3, 1),
                date(2024, 3, 31),
                date(2022, 2, 28),
                date(2023, 1, 31),
                date(2019, 12, 31),
            ],
            "month_delta": [None, None, None, None, None],
        },
        schema_overrides={"month_delta": pl.Int32},
    )
    assert_frame_equal(result, expected)


def test_month_delta_nulls() -> None:
    df = pl.DataFrame(
        {
            "start_date": [
                date(2024, 3, 1),
                None,
                None,
                date(2023, 1, 31),
                date(2019, 12, 31),
            ],
            "end_date": [
                None,
                date(2024, 3, 31),
                None,
                date(2023, 1, 31),
                date(2019, 12, 31),
            ],
        },
    )
    result = df.with_columns(
        xdt.month_delta("start_date", "end_date").alias("month_delta")
    )["month_delta"].to_list()
    expected = [None, None, None, 0, 0]
    assert result == expected
