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
        ewma=xdt.ewma_by_time(
            "values", times="times", halflife=timedelta(days=4)
        ),
    )
    expected = pl.DataFrame(
        {
            "ewma": [
                0.0,
                0.585786437626905,
                1.52388878049859,
                None,
                3.2336858398518338,
            ]
        }
    )
    assert_frame_equal(result, expected)
