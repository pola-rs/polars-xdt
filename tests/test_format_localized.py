import pytest

from datetime import datetime, date

import polars as pl
import polars_xdt  # noqa: F401

@pytest.mark.parametrize('time_unit', ['ms', 'us', 'ns'])
def test_format_localized_datetime(time_unit: str) -> None:
    df = pl.DataFrame(
        {
            "date_col": [datetime(2024, 8, 24), datetime(2024, 10, 1)],
        },
        schema={'date_col': pl.Datetime(time_unit)}
    )
    result = df.select(result=pl.col("date_col").xdt.format_localized("%A, %d %B %Y", 'en_US'))['result']
    assert result[0] == 'Saturday, 24 August 2024'
    assert result[1] == 'Tuesday, 01 October 2024'
    result = df.select(result=pl.col("date_col").xdt.format_localized("%A, %d %B %Y", 'uk_UA'))['result']
    assert result[0] == 'субота, 24 серпня 2024'
    assert result[1] == 'вівторок, 01 жовтня 2024'
