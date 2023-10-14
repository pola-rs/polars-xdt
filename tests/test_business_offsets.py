import datetime as dt
import pandas as pd

import hypothesis.strategies as st
import numpy as np
from hypothesis import given, assume, reject

import polars as pl
import polars_business  # noqa: F401


@given(
    date=st.dates(min_value=dt.date(1000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
)
def test_against_np_busday_offset(date: dt.date, n: int) -> None:
    assume(date.weekday() < 5)
    result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(n=n))['ts'].item()
    expected = np.busday_offset(date, n)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
)
def test_against_pandas_bday_offset(date: dt.date, n: int) -> None:
    assume(date.weekday() < 5)
    result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(n=n))['ts'].item()
    expected = pd.Timestamp(date) + pd.tseries.offsets.BusinessDay(n)
    assert pd.Timestamp(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
)
def test_bday_n_expression(date: dt.date, n: int) -> None:
    assume(date.weekday() < 5)
    result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(n=pl.Series([n])))['ts'].item()
    expected = pd.Timestamp(date) + pd.tseries.offsets.BusinessDay(n)
    assert pd.Timestamp(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    holidays = st.lists(st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)), min_size=1, max_size=300)
)
def test_against_np_busday_offset_with_holidays(date: dt.date, n: int, holidays: list[dt.date]) -> None:
    assume(date.weekday() < 5)
    assume(date not in holidays)  # TODO: remove once unwrap is removed
    try:
        result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(
            n=n,
            holidays=holidays
            ))['ts'].item()
    except pl.PolarsPanicError as exc:
        assert date in holidays
        assert 'cannot advance' in str(exc)
        reject()
    expected = np.busday_offset(date, n, holidays=holidays)
    assert np.datetime64(result) == expected

@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    weekend = st.lists(st.sampled_from([0,1,2,3,4,5,6]), min_size=0, max_size=7)
)
def test_against_np_busday_offset_with_weekends(date: dt.date, n: int, weekend: list[dt.date]) -> None:
    assume(date.weekday() < 5)
    assume(date.weekday() not in weekend)  # TODO: remove once unwrap is removed
    try:
        result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(
            n=n,
            weekend=weekend,
            ))['ts'].item()
    except pl.PolarsPanicError as exc:
        assert date.weekend() in weekend
        assert 'cannot advance' in str(exc)
        reject()

    weekmask = [0 if i in weekend else 1 for i in range(7)]
    expected = np.busday_offset(date, n, weekmask=weekmask)
    assert np.datetime64(result) == expected

@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    weekend = st.lists(st.sampled_from([0,1,2,3,4,5,6]), min_size=0, max_size=7),
    holidays = st.lists(st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)), min_size=1, max_size=300)
)
def test_against_np_busday_offset_with_weekends_and_holidays(date: dt.date, n: int, weekend: list[int], holidays: list[dt.date]) -> None:
    assume(date.weekday() < 5)
    assume(date.weekday() not in weekend)  # TODO: remove once unwrap is removed
    assume(date not in holidays)  # TODO: remove once unwrap is removed
    try:
        result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(
            n=n,
            weekend=weekend,
            holidays=holidays,
            ))['ts'].item()
    except pl.PolarsPanicError as exc:
        assert date.weekend() in weekend
        assert 'cannot advance' in str(exc)
        reject()

    weekmask = [0 if i in weekend else 1 for i in range(7)]
    expected = np.busday_offset(date, n, weekmask=weekmask, holidays=holidays)
    assert np.datetime64(result) == expected
