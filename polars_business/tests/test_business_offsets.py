import datetime as dt
import pytest
import pandas as pd  # type: ignore
from typing import Mapping, Any, Callable

import hypothesis.strategies as st
import numpy as np
from hypothesis import given, assume, reject

import polars as pl
import polars_business as plb
from polars.type_aliases import PolarsDataType


mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}


def get_result(
    date: dt.date,
    dtype: PolarsDataType,
    by: str | pl.Series,
    **kwargs: Mapping[str, Any],
) -> dt.date:
    if dtype == pl.Date:
        result = (
            pl.DataFrame({"ts": [date]})
            .select(plb.col("ts").bdt.offset_by(by=by, **kwargs))["ts"]  # type: ignore[arg-type]
            .item()
        )
    else:
        try:
            result = (
                pl.DataFrame({"ts": [dt.datetime(date.year, date.month, date.day)]})
                .select(
                    pl.col("ts")
                    .dt.cast_time_unit(dtype.time_unit)  # type: ignore[union-attr]
                    .dt.replace_time_zone(dtype.time_zone)  # type: ignore[union-attr]
                    .bdt.offset_by(by=by, **kwargs)  # type: ignore[attr-defined]
                    .dt.date()
                )["ts"]
                .item()
            )
        except Exception as exp:
            assert "non-existent" in str(exp) or "ambiguous" in str(exp)
            reject()
    return result  # type: ignore[no-any-return]


@given(
    date=st.dates(min_value=dt.date(1000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    dtype=st.sampled_from(
        [
            pl.Date,
            pl.Datetime("ms"),
            pl.Datetime("ms", "Asia/Kathmandu"),
            pl.Datetime("us", "Europe/London"),
        ]
    ),
    function=st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset(
    date: dt.date,
    n: int,
    dtype: PolarsDataType,
    function: Callable[[str], str | pl.Series],
) -> None:
    # how to do this...
    # convert time zone of date
    assume(date.strftime("%a") not in ("Sat", "Sun"))
    result = get_result(date, dtype, by=function(f"{n}bd"))
    expected = np.busday_offset(date, n)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(9999, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
)
def test_against_pandas_bday_offset(date: dt.date, n: int) -> None:
    # maybe just remove this one?
    assume(date.strftime("%a") not in ("Sat", "Sun"))
    result = (
        pl.DataFrame({"ts": [date]})
        .select(plb.col("ts").bdt.offset_by(by=f"{n}bd"))["ts"]
        .item()
    )
    expected = pd.Timestamp(date) + pd.tseries.offsets.BusinessDay(n)
    assert pd.Timestamp(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    holidays=st.lists(
        st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
        min_size=1,
        max_size=300,
    ),
    dtype=st.sampled_from(
        [
            pl.Date,
            pl.Datetime("ms"),
            pl.Datetime("ms", "Asia/Kathmandu"),
            pl.Datetime("us", "Europe/London"),
        ]
    ),
    function=st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset_with_holidays(
    date: dt.date,
    n: int,
    holidays: list[dt.date],
    dtype: PolarsDataType,
    function: Callable[[str], str | pl.Series],
) -> None:
    assume(date.strftime("%a") not in ("Sat", "Sun"))
    assume(date not in holidays)  # TODO: remove once unwrap is removed
    result = get_result(date, dtype, by=function(f"{n}bd"), holidays=holidays)  # type: ignore[arg-type]
    expected = np.busday_offset(date, n, holidays=holidays)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    weekend=st.lists(
        st.sampled_from(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]),
        min_size=0,
        max_size=7,
    ),
    dtype=st.sampled_from(
        [
            pl.Date,
            pl.Datetime("ms"),
            pl.Datetime("ms", "Asia/Kathmandu"),
            pl.Datetime("us", "Europe/London"),
        ]
    ),
    function=st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset_with_weekends(
    date: dt.date,
    n: int,
    weekend: list[dt.date],
    dtype: PolarsDataType,
    function: Callable[[str], str | pl.Series],
) -> None:
    assume(date.strftime("%a") not in weekend)
    result = get_result(date, dtype, by=function(f"{n}bd"), weekend=weekend)  # type: ignore[arg-type]
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    expected = np.busday_offset(date, n, weekmask=weekmask)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
    n=st.integers(min_value=-30, max_value=30),
    weekend=st.lists(
        st.sampled_from(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]),
        min_size=0,
        max_size=7,
    ),
    holidays=st.lists(
        st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)),
        min_size=1,
        max_size=300,
    ),
    dtype=st.sampled_from(
        [
            pl.Date,
            pl.Datetime("ms"),
            pl.Datetime("ms", "Asia/Kathmandu"),
            pl.Datetime("us", "Europe/London"),
        ]
    ),
    function=st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset_with_weekends_and_holidays(
    date: dt.date,
    n: int,
    weekend: list[str],
    holidays: list[dt.date],
    dtype: PolarsDataType,
    function: Callable[[str], str | pl.Series],
) -> None:
    assume(date.strftime("%a") not in weekend)
    assume(date not in holidays)
    result = get_result(
        date, dtype, by=function(f"{n}bd"), weekend=weekend, holidays=holidays  # type: ignore[arg-type]
    )
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    expected = np.busday_offset(date, n, weekmask=weekmask, holidays=holidays)
    assert np.datetime64(result) == expected


@pytest.mark.parametrize(
    ("by", "expected"),
    [
        ("1bd", dt.datetime(2000, 1, 4)),
        ("2bd", dt.datetime(2000, 1, 5)),
        ("1bd2h", dt.datetime(2000, 1, 4, 2)),
        ("2h1bd", dt.datetime(2000, 1, 4, 2)),
        ("2bd1h", dt.datetime(2000, 1, 5, 1)),
        ("-1bd", dt.datetime(1999, 12, 31)),
        ("-2bd", dt.datetime(1999, 12, 30)),
        ("-1bd2h", dt.datetime(1999, 12, 30, 22)),
        ("-2h1bd", dt.datetime(1999, 12, 30, 22)),
        ("-2bd1h", dt.datetime(1999, 12, 29, 23)),
    ],
)
def test_extra_args(by: str, expected: dt.datetime) -> None:
    start = dt.datetime(2000, 1, 3)
    df = pl.DataFrame({"dates": [start]})
    result = (
        df.with_columns(
            dates_shifted=plb.col("dates").bdt.offset_by(by=by)
        ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
    )["dates_shifted"].item()
    assert result == expected


def test_extra_args_w_series() -> None:
    start = dt.datetime(2000, 1, 3)
    df = pl.DataFrame({"dates": [start] * 2, "by": ["1bd2h", "-1bd1h"]})
    result = (
        df.with_columns(
            dates_shifted=plb.col("dates").bdt.offset_by(by=pl.col("by"))
        ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
    )["dates_shifted"]
    assert result[0] == dt.datetime(2000, 1, 4, 2)
    assert result[1] == dt.datetime(1999, 12, 30, 23)


def test_starting_on_non_business() -> None:
    start = dt.date(2000, 1, 1)
    n = -7
    weekend = ["Sat", "Sun"]
    holidays = []
    df = pl.DataFrame({"dates": [start]})
    with pytest.raises(pl.ComputeError):
        df.with_columns(
            dates_shifted=plb.col("dates").bdt.offset_by(
                by=f"{n}bd",
                holidays=holidays,
                weekend=weekend,
            )
        )

    df = pl.DataFrame({"dates": [start]})
    weekend = []
    holidays = [start]
    with pytest.raises(pl.ComputeError):
        df.with_columns(
            dates_shifted=plb.col("dates").bdt.offset_by(
                by=f"{n}bd",
                holidays=holidays,
                weekend=weekend,
            )
        )
