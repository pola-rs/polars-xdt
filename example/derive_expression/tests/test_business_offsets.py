import datetime as dt
import pandas as pd

import hypothesis.strategies as st
import numpy as np
from hypothesis import given, assume, reject

import polars as pl
import polars_business  # noqa: F401


reverse_mapping = {value: key for key, value in polars_business.mapping.items()}

def get_result(date, dtype, **kwargs):
    if dtype == pl.Date:
        result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(**kwargs))['ts'].item()
    else:
        result = pl.DataFrame({'ts': [dt.datetime(date.year, date.month, date.day)]}).select(
            pl.col('ts').dt.cast_time_unit(dtype.time_unit)
            .dt.replace_time_zone(dtype.time_zone)
            .business.advance_n_days(**kwargs)
            .dt.date()
        )['ts'].item()
    return result


@given(
    date=st.dates(min_value=dt.date(1000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    dtype = st.sampled_from([pl.Date, pl.Datetime('ms'), pl.Datetime('ms', 'Asia/Kathmandu'), pl.Datetime('us', 'Europe/London')]),
    function = st.sampled_from([lambda x: x, lambda x: pl.Series([x])])
)
def test_against_np_busday_offset(date: dt.date, n: int, dtype, function) -> None:
    # how to do this...
    # convert time zone of date
    assume(date.weekday() < 5)
    result = get_result(date, dtype, n=function(n))
    expected = np.busday_offset(date, n)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
)
def test_against_pandas_bday_offset(date: dt.date, n: int) -> None:
    # maybe just remove this one?
    assume(date.weekday() < 5)
    result = pl.DataFrame({'ts': [date]}).select(pl.col('ts').business.advance_n_days(n=n))['ts'].item()
    expected = pd.Timestamp(date) + pd.tseries.offsets.BusinessDay(n)
    assert pd.Timestamp(result) == expected



@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    holidays = st.lists(st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)), min_size=1, max_size=300),
    dtype = st.sampled_from([pl.Date, pl.Datetime('ms'), pl.Datetime('ms', 'Asia/Kathmandu'), pl.Datetime('us', 'Europe/London')]),
    function = st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset_with_holidays(date: dt.date, n: int, holidays: list[dt.date], dtype, function) -> None:
    assume(date.weekday() < 5)
    assume(date not in holidays)  # TODO: remove once unwrap is removed
    result = get_result(date, dtype, n=function(n), holidays=holidays)
    expected = np.busday_offset(date, n, holidays=holidays)
    assert np.datetime64(result) == expected

@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    weekend = st.lists(st.sampled_from(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']), min_size=0, max_size=7),
    dtype = st.sampled_from([pl.Date, pl.Datetime('ms'), pl.Datetime('ms', 'Asia/Kathmandu'), pl.Datetime('us', 'Europe/London')]),
    function = st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset_with_weekends(date: dt.date, n: int, weekend: list[dt.date], dtype, function) -> None:
    assume(reverse_mapping[date.weekday()] not in weekend)
    result = get_result(date, dtype, n=function(n), weekend=weekend)
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(7)]
    expected = np.busday_offset(date, n, weekmask=weekmask)
    assert np.datetime64(result) == expected

@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    weekend = st.lists(st.sampled_from(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']), min_size=0, max_size=7),
    holidays = st.lists(st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)), min_size=1, max_size=300),
    dtype = st.sampled_from([pl.Date, pl.Datetime('ms'), pl.Datetime('ms', 'Asia/Kathmandu'), pl.Datetime('us', 'Europe/London')]),
    function = st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset_with_weekends_and_holidays(date: dt.date, n: int, weekend: list[int], holidays: list[dt.date], dtype, function) -> None:
    assume(reverse_mapping[date.weekday()] not in weekend)  # TODO: remove once unwrap is removed
    assume(date not in holidays)  # TODO: remove once unwrap is removed
    result = get_result(date, dtype, n=function(n), weekend=weekend, holidays=holidays)
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(7)]
    expected = np.busday_offset(date, n, weekmask=weekmask, holidays=holidays)
    assert np.datetime64(result) == expected
