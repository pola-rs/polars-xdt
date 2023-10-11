import datetime as dt
import pandas as pd

import hypothesis.strategies as st
import numpy as np
from hypothesis import given, assume

import polars as pl
from polars_business_day_tools import BusinessDayTools


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=1, max_value=30),
)
def test_against_np_busday_offset(date: dt.date, n: int) -> None:
    assume(date.weekday() < 5)
    result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').bdt.advance_by_days(n=n))['ts'].item()
    expected = np.busday_offset(date, n)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=1, max_value=30),
)
def test_against_pandas_bday_offset(date: dt.date, n: int) -> None:
    result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').bdt.advance_by_days(n=n))['ts'].item()
    expected = pd.Timestamp(date) + pd.tseries.offsets.BusinessDay(n)
    assert pd.Timestamp(result) == expected

