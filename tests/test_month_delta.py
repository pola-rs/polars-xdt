from datetime import date

import polars as pl
from dateutil.relativedelta import relativedelta
from hypothesis import example, given
from hypothesis import strategies as st

import polars_xdt as xdt


@given(
    start_date=st.dates(
        min_value=date(1960, 1, 1), max_value=date(2024, 12, 31)
    ),
    end_date=st.dates(min_value=date(1960, 1, 1), max_value=date(2024, 12, 31)),
)
@example(start_date=date(2022, 2, 28), end_date=date(2024, 2, 29))  # Leap year
@example(start_date=date(2024, 1, 1), end_date=date(2024, 1, 31))  # Same month
@example(start_date=date(1973, 1, 1), end_date=date(1973, 1, 1))  # Same date
@example(start_date=date(2019, 12, 31), end_date=date(2020, 1, 1))  # Border
@example(start_date=date(2018, 12, 1), end_date=date(2020, 1, 1))  # End of year
@example(start_date=date(2022, 12, 1), end_date=date(2020, 1, 1))  # Negative
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
        while True:
            start_date = start_date + relativedelta(months=1)
            if start_date > end_date:
                break
            expected += 1
    else:
        while True:
            end_date = end_date + relativedelta(months=1)
            if end_date > start_date:
                break
            expected -= 1

    assert result == expected
