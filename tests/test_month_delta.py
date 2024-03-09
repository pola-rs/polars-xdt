import polars as pl
import polars_xdt as xdt
from datetime import date
from dateutil.relativedelta import relativedelta

from hypothesis import given, strategies as st, assume


def test_month_delta():
    df = pl.DataFrame(
        {
            "start_date": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2023, 9, 1),
                date(2023, 1, 4),
                date(2022, 6, 4),
                date(2023, 1, 1),
                date(2023, 1, 1),
                date(2022, 2, 1),
                date(2022, 2, 1),
                date(2024, 3, 1),
                date(2024, 3, 31),
                date(2022, 2, 28),
                date(2023, 1, 31),
                date(2019, 12, 31),
                date(2024, 1, 31),
                date(1970, 1, 2),
            ],
            "end_date": [
                date(2024, 1, 4),
                date(2024, 1, 31),
                date(2023, 11, 1),
                date(2022, 1, 4),
                date(2022, 1, 4),
                date(2022, 12, 31),
                date(2021, 12, 31),
                date(2022, 3, 1),
                date(2023, 3, 1),
                date(2023, 2, 28),
                date(2023, 2, 28),
                date(2023, 1, 31),
                date(2022, 2, 28),
                date(2023, 1, 1),
                date(2024, 4, 30),
                date(1971, 1, 1),
            ],
        },
    )

    assert_month_diff = [
        0,  # 2024-01-01 to 2024-01-04
        0,  # 2024-01-01 to 2024-01-31
        2,  # 2023-09-01 to 2023-11-01
        -12,  # 2023-01-04 to 2022-01-04
        -5,  # 2022-06-04 to 2022-01-04
        0,  # 2023-01-01 to 2022-12-31
        -12,  # 2023-01-01 to 2021-12-31
        1,  # 2022-02-01 to 2022-03-01
        13,  # 2022-02-01 to 2023-03-01
        -12,  # 2024-03-01 to 2023-02-28
        -13,  # 2024-03-31 to 2023-02-28
        11,  # 2022-02-28 to 2023-01-31
        -11,  # 2023-01-31 to 2022-02-28
        36,  # 2019-12-31 to 2023-01-01
        3,  # 2024-01-31 to 2024-04-30
        11,  # 1970-01-02 to 1971-01-01
    ]
    df = df.with_columns(
        # For easier visual debugging purposes
        pl.Series(name="assert_month_delta", values=assert_month_diff),
        month_delta=xdt.month_delta("start_date", "end_date"),
    )
    # pl.Config.set_tbl_rows(50)
    # print(df)
    month_diff_list = df.get_column("month_delta").to_list()
    assert assert_month_diff == month_diff_list, (
        "The month difference list did not match the expected values.\n"
        "Please check the function: 'month_diff.rs' for discrepancies."
    )


@given(
    start_date=st.dates(
        min_value=date(1960, 1, 1), max_value=date(2024, 12, 31)
    ),
    end_date=st.dates(min_value=date(1960, 1, 1), max_value=date(2024, 12, 31)),
)
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
