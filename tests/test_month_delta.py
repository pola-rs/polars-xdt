import polars as pl
import polars_xdt as xdt
from datetime import date


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

    expected_month_diff = [
        0,
        0,
        2,
        -12,
        -5,
        0,
        -12,
        1,
        13,
        -12,
        -14,
        12,
        -12,
        36,
        3,
        11,
    ]
    df = df.with_columns(
        # For easier visual debugging purposes
        pl.Series(name="out_month_delta", values=expected_month_diff),
        month_delta=xdt.month_delta("start_date", "end_date"),
    )

    month_diff_list = df.get_column("month_delta").to_list()

    assert expected_month_diff == month_diff_list, (
        "The month difference list did not match the expected values.\n"
        "Please check the function: 'month_diff.rs' for discrepancies."
    )
