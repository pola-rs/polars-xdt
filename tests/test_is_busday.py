from __future__ import annotations
import datetime as dt

import hypothesis.strategies as st
import numpy as np
from hypothesis import given

import polars as pl
import polars_xdt as xdt


mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}


def get_result(
    date: dt.date,
    weekend: list[str],
    holidays: list[dt.date],
) -> int:
    return (  # type: ignore[no-any-return]
        pl.DataFrame({"date": [date]})
        .select(xdt.is_workday("date", weekend=weekend, holidays=holidays))[
            "date"
        ]
        .item()
    )


@given(
    date=st.dates(
        min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)
    ),
    weekend=st.lists(
        st.sampled_from(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]),
        min_size=0,
        max_size=6,
        unique=True,
    ),
    holidays=st.lists(
        st.dates(
            min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)
        ),
        min_size=1,
        max_size=300,
    ),
)
def test_against_np_is_busday(
    date: dt.date,
    weekend: list[str],
    holidays: list[dt.date],
) -> None:
    result = get_result(date, weekend=weekend, holidays=holidays)
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    expected = np.is_busday(date, weekmask=weekmask, holidays=holidays)
    assert result == expected


@given(
    datetime=st.datetimes(
        min_value=dt.datetime(2000, 1, 1), max_value=dt.datetime(2000, 12, 31)
    ),
    weekend=st.lists(
        st.sampled_from(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]),
        min_size=0,
        max_size=6,
        unique=True,
    ),
    holidays=st.lists(
        st.dates(
            min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)
        ),
        min_size=1,
        max_size=300,
    ),
)
def test_against_np_is_busday_datetime(
    datetime: dt.datetime,
    weekend: list[str],
    holidays: list[dt.date],
) -> None:
    result = get_result(datetime, weekend=weekend, holidays=holidays)
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    date = dt.date(datetime.year, datetime.month, datetime.day)
    expected = np.is_busday(date, weekmask=weekmask, holidays=holidays)
    assert result == expected
