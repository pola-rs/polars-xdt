from __future__ import annotations

import datetime as dt
from polars.testing import assert_frame_equal
import pytest
from typing import Mapping, Any, Callable, Literal

import hypothesis.strategies as st
import numpy as np
from hypothesis import given, assume, reject

import polars as pl
import polars_xdt as xdt
from polars.type_aliases import PolarsDataType


mapping = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
reverse_mapping = {value: key for key, value in mapping.items()}


def get_result(
    date: dt.date,
    dtype: PolarsDataType,
    by: str | pl.Series,
    **kwargs: Any,
) -> dt.date:
    if dtype == pl.Date:
        result = (
            pl.DataFrame({"ts": [date]})
            .select(xdt.offset_by("ts", by=by, **kwargs))["ts"]  # type: ignore[arg-type]
            .item()
        )
    else:
        try:
            result = (
                pl.DataFrame(
                    {"ts": [dt.datetime(date.year, date.month, date.day)]}
                )
                .select(
                    xdt.offset_by(
                        pl.col("ts")
                        .dt.cast_time_unit(dtype.time_unit)  # type: ignore[union-attr]
                        .dt.replace_time_zone(dtype.time_zone),  # type: ignore[union-attr]
                        by=by,
                        **kwargs,  # type: ignore[arg-type]
                    ).dt.date()
                )["ts"]
                .item()
            )
        except Exception as exp:
            assert "non-existent" in str(exp) or "ambiguous" in str(exp)
            reject()
    return result  # type: ignore[no-any-return]


@given(
    date=st.dates(
        min_value=dt.date(1969, 1, 1), max_value=dt.date(1971, 12, 31)
    ),
    n=st.integers(min_value=-30, max_value=30),
    weekend=st.lists(
        st.sampled_from(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]),
        min_size=0,
        max_size=6,
        unique=True,
    ),
    holidays=st.lists(
        st.dates(
            min_value=dt.date(1969, 1, 1), max_value=dt.date(1971, 12, 31)
        ),
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
def test_against_np_busday_offset(
    date: dt.date,
    n: int,
    weekend: list[str],
    holidays: list[dt.date],
    dtype: PolarsDataType,
    function: Callable[[str], str | pl.Series],
) -> None:
    assume(date.strftime("%a") not in weekend)
    assume(date not in holidays)
    roll = "raise"
    result = get_result(
        date,
        dtype,
        by=function(f"{n}bd"),
        weekend=weekend,
        holidays=holidays,
        roll=roll,
    )
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    expected = np.busday_offset(date, n, weekmask=weekmask, holidays=holidays)
    assert np.datetime64(result) == expected


@given(
    date=st.dates(
        min_value=dt.date(1969, 1, 1), max_value=dt.date(1971, 12, 31)
    ),
    n=st.integers(min_value=-30, max_value=30),
    weekend=st.lists(
        st.sampled_from(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]),
        min_size=0,
        max_size=6,
        unique=True,
    ),
    holidays=st.lists(
        st.dates(
            min_value=dt.date(1969, 1, 1), max_value=dt.date(1971, 12, 31)
        ),
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
    roll=st.sampled_from(["forward", "backward"]),
)
def test_against_np_busday_offset_with_roll(
    date: dt.date,
    n: int,
    weekend: list[str],
    holidays: list[dt.date],
    dtype: PolarsDataType,
    function: Callable[[str], str | pl.Series],
    roll: Literal["forward", "backward"],
) -> None:
    result = get_result(
        date,
        dtype,
        by=function(f"{n}bd"),
        weekend=weekend,
        holidays=holidays,
        roll=roll,  # type: ignore[arg-type]
    )
    weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]
    expected = np.busday_offset(
        date, n, weekmask=weekmask, holidays=holidays, roll=roll
    )
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
            dates_shifted=xdt.offset_by("dates", by=by)
        ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
    )["dates_shifted"].item()
    assert result == expected


def test_extra_args_w_series() -> None:
    start = dt.datetime(2000, 1, 3)
    df = pl.DataFrame({"dates": [start] * 2, "by": ["1bd2h", "-1bd1h"]})
    result = (
        df.with_columns(
            dates_shifted=xdt.offset_by("dates", by=pl.col("by"))
        ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
    )["dates_shifted"]
    assert result[0] == dt.datetime(2000, 1, 4, 2)
    assert result[1] == dt.datetime(1999, 12, 30, 23)


def test_starting_on_non_business() -> None:
    start = dt.date(2000, 1, 1)
    n = -7
    weekend = ["Sat", "Sun"]
    df = pl.DataFrame({"dates": [start]})
    with pytest.raises(pl.ComputeError):
        df.with_columns(
            dates_shifted=xdt.offset_by(
                "dates",
                by=f"{n}bd",
                weekend=weekend,
            )
        )

    df = pl.DataFrame({"dates": [start]})
    weekend = []
    holidays = [start]
    with pytest.raises(pl.ComputeError):
        df.with_columns(
            dates_shifted=xdt.offset_by(
                "dates",
                by=f"{n}bd",
                holidays=holidays,
                weekend=weekend,
            )
        )


def test_within_group_by() -> None:
    data = {
        "a": [1, 2],
        "date": [dt.datetime(2022, 2, 1), dt.datetime(2023, 2, 1)],
    }
    df = pl.DataFrame(data)

    result = (
        df.group_by(["a"]).agg(
            minDate=xdt.offset_by(pl.col.date.min(), "-3bd"),  # type: ignore[attr-defined]
            maxDate=xdt.offset_by(pl.col.date.max(), "3bd"),  # type: ignore[attr-defined]
        )
    ).sort("a", descending=True)
    expected = pl.DataFrame(
        {
            "a": [2, 1],
            "minDate": [dt.datetime(2023, 1, 27), dt.datetime(2022, 1, 27)],
            "maxDate": [dt.datetime(2023, 2, 6), dt.datetime(2022, 2, 4)],
        }
    )
    assert_frame_equal(result, expected)


def test_invalid_roll_strategy() -> None:
    df = pl.DataFrame(
        {
            "date": pl.date_range(
                dt.date(2023, 12, 1), dt.date(2023, 12, 5), eager=True
            )
        }
    )
    with pytest.raises(pl.ComputeError):
        df.with_columns(xdt.offset_by("date", "1bd", roll="cabbage"))  # type: ignore[arg-type]
