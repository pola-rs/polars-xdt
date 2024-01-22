from __future__ import annotations
import datetime as dt
import pytest
from typing import Callable

import hypothesis.strategies as st
import numpy as np
from hypothesis import given, assume, reject

import polars as pl
import polars_xdt as xdt


mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}


def get_result(
    start_date: dt.date | pl.Series,
    end_date: dt.date,
    weekend: list[str],
    holidays: list[dt.date],
) -> int:
    return (  # type: ignore[no-any-return]
        pl.DataFrame({"end_date": [end_date]})
        .select(
            n=xdt.workday_count(
                start_date, "end_date", weekend=weekend, holidays=holidays
            )
        )["n"]  # type: ignore[arg-type]
        .item()
    )


@given(
    start_date=st.dates(
        min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)
    ),
    end_date=st.dates(
        min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)
    ),
    function=st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
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
def test_against_np_busday_count(
    start_date: dt.date,
    end_date: dt.date,
    weekend: list[str],
    holidays: list[dt.date],
    function: Callable[[dt.date], dt.date | pl.Series],
) -> None:
    result = get_result(
        function(start_date), end_date, weekend=weekend, holidays=holidays
    )
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    expected = np.busday_count(
        start_date, end_date, weekmask=weekmask, holidays=holidays
    )
    if start_date > end_date and tuple(
        int(v) for v in np.__version__.split(".")[:2]
    ) < (1, 25):
        # Bug in old versions of numpy
        reject()
    assert result == expected


@given(
    start_date=st.dates(
        min_value=dt.date(2000, 1, 1), max_value=dt.date(2000, 12, 31)
    ),
    end_date=st.dates(
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
def test_against_naive_python(
    start_date: dt.date,
    end_date: dt.date,
    weekend: list[str],
    holidays: list[dt.date],
) -> None:
    assume(end_date > start_date)
    result = get_result(
        start_date, end_date, weekend=weekend, holidays=holidays
    )
    expected = 0
    start_date_copy = start_date
    while start_date_copy < end_date:
        if start_date_copy.strftime("%a") in weekend:
            start_date_copy += dt.timedelta(days=1)
            continue
        if start_date_copy in holidays:
            start_date_copy += dt.timedelta(days=1)
            continue
        start_date_copy += dt.timedelta(days=1)
        expected += 1
    assert result == expected


def test_empty_weekmask() -> None:
    df = pl.DataFrame(
        {
            "start": [dt.date(2020, 1, 1)],
            "end": [dt.date(2020, 1, 5)],
        }
    )
    with pytest.raises(ValueError):
        df.select(
            xdt.workday_count(
                "start",
                "end",
                weekend=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            )
        )


def test_sub_lit() -> None:
    df = pl.DataFrame(
        {
            "end": [dt.date(2020, 1, 3), dt.date(2020, 1, 5)],
        }
    )
    result = df.select(xdt.workday_count(pl.lit(dt.date(2020, 1, 1)), "end"))
    assert result["literal"][0] == 2
    assert result["literal"][1] == 3


def test_workday_count() -> None:
    df = pl.DataFrame(
        {
            "start": [dt.date(2020, 1, 3), dt.date(2020, 1, 5)],
            "end": [dt.date(2020, 1, 8), dt.date(2020, 1, 20)],
        }
    )
    result = df.with_columns(workday_count=xdt.workday_count("start", "end"))
    assert result["workday_count"][0] == 3
    assert result["workday_count"][1] == 10
    result = df.with_columns(
        workday_count=xdt.workday_count("start", dt.date(2020, 1, 8))
    )
    assert result["workday_count"][0] == 3
    assert result["workday_count"][1] == 2
    result = df.with_columns(
        workday_count=xdt.workday_count(dt.date(2020, 1, 5), pl.col("end"))
    )
    assert result["workday_count"][0] == 2
    assert result["workday_count"][1] == 10
