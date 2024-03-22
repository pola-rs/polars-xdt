import polars as pl
from polars.testing import assert_frame_equal
import polars_xdt as xdt
from datetime import datetime, timedelta


def test_ewma_by_time():
    df = pl.DataFrame(
        {
            "values": [0.0, 1, 2, None, 4],
            "times": [
                datetime(2020, 1, 1),
                datetime(2020, 1, 3),
                datetime(2020, 1, 10),
                datetime(2020, 1, 15),
                datetime(2020, 1, 17),
            ],
        }
    )
    result = df.select(
        xdt.ewma_by_time("values", times="times", half_life=timedelta(days=4)),
    )
    expected = pl.DataFrame(
        {
            "values": [
                0.0,
                0.2928932188134524,
                1.4924741174358913,
                None,
                3.2545080948503213,
            ]
        }
    )
    assert_frame_equal(result, expected)
