import polars as pl
from polars.testing import assert_frame_equal
import polars_xdt as xdt
import pytest
from datetime import datetime, timedelta

import os

os.environ["POLARS_VERBOSE"] = "1"

@pytest.mark.parametrize("start_null", [True, False])
def test_ewma_by_time(start_null):
    if start_null:
        values = [None]
        times = [datetime(2020, 1, 1)]
    else:
        values = []
        times = []
        
    df = pl.DataFrame(
        {
            "values": values + [0.0, 1., 2., None, 4.],
            "times": times + [
                datetime(2020, 1, 2),
                datetime(2020, 1, 4),
                datetime(2020, 1, 11),
                datetime(2020, 1, 16),
                datetime(2020, 1, 18),
            ],
        }
    )
    result = df.select(
        xdt.ewma_by_time("values", times="times", half_life=timedelta(days=2)),
    )
        
    expected = pl.DataFrame(
        {
            "values": values + [
                0.0,
                0.5,
                1.8674174785275222,
                None,
                3.811504554703363,
            ]
        }
    )

    assert_frame_equal(result, expected)
