from datetime import datetime
import polars as pl
import polars_xdt as xdt


def test_ceil() -> None:
    df = pl.DataFrame(
        {
            "date_col": [
                datetime(2024, 8, 24, 1, 2, 3, 123456),
                datetime(2024, 10, 1),
            ],
        },
        schema={"date_col": pl.Datetime("ms")},
    )
    result = df.select(result=xdt.ceil("date_col", "1mo"))["result"]
    assert result[0] == datetime(2024, 9, 1, 0, 0, 0, 0)
    assert result[1] == datetime(2024, 10, 1, 0, 0, 0, 0)
