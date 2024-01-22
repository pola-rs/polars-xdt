from __future__ import annotations
import datetime as dt

import hypothesis.strategies as st
from hypothesis import given

import polars as pl
import pandas as pd
import polars_xdt as xdt


@given(
    date=st.datetimes(
        min_value=dt.datetime(1, 1, 1), max_value=dt.datetime(9999, 12, 31)
    ),
)
def test_against_pandas(
    date: dt.date,
) -> None:
    df = pl.DataFrame({"a": [date]}, schema={"a": pl.Datetime("ms")})
    result = df.select(xdt.to_julian_date("a"))["a"].item()
    expected = pd.Timestamp(df["a"].item()).to_julian_date()
    assert result == expected


@given(
    date=st.dates(min_value=dt.date(1, 1, 1), max_value=dt.date(9999, 12, 31)),
)
def test_against_pandas_date(
    date: dt.date,
) -> None:
    df = pl.DataFrame({"a": [date]})
    result = df.select(xdt.to_julian_date("a"))["a"].item()
    expected = pd.Timestamp(df["a"].item()).to_julian_date()
    assert result == expected
