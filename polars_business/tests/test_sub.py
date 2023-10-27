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
    start_date: dt.date | pl.Series, end_date: dt.date, **kwargs: Mapping[str, Any]
) -> int:
    return (  # type: ignore[no-any-return]
        pl.DataFrame({"end_date": [end_date]})
        .select(n=plb.col("end_date").bdt.sub(start_date, **kwargs))["n"]  # type: ignore[arg-type]
        .item()
    )


@given(
    start_date=st.dates(min_value=dt.date(1000, 1, 1), max_value=dt.date(9999, 12, 31)),
    end_date=st.dates(min_value=dt.date(1000, 1, 1), max_value=dt.date(9999, 12, 31)),
    function=st.sampled_from([lambda x: x, lambda x: pl.Series([x])]),
)
def test_against_np_busday_offset(
    start_date: dt.date,
    end_date: dt.date,
    function: Callable[[dt.date], dt.date | pl.Series],
) -> None:
    result = get_result(function(start_date), end_date)
    expected = np.busday_count(start_date, end_date)
    assert result == expected
