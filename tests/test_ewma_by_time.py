import polars as pl
from polars.testing import assert_frame_equal
import polars_xdt as xdt
import pytest
from datetime import datetime, timedelta

import os

os.environ["POLARS_VERBOSE"] = "1"

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
        xdt.ewma_by_time("values", times="times", half_life=timedelta(days=4), ignore_nulls=False),
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


@pytest.mark.parametrize("ignore_nulls", [True, False])
@pytest.mark.parametrize("start_null", [True, False])
def test_ewma_with_nan(ignore_nulls, start_null):
    n = 6

    df = pl.DataFrame({
        "values": list(range(n)),
        "times": [datetime(2020, 1, 1) + timedelta(days=i) for i in range(n)]
    })
    
    when = pl.col("values") > n//2
    
    if start_null:
        when = when.or_(pl.col("values") == 0)

    result = df.select(xdt.ewma_by_time(
        pl.when(when).then(None).otherwise(pl.col("values")), times="times", half_life=timedelta(days=1), ignore_nulls=ignore_nulls
    ))
    
    if ignore_nulls & start_null:
            expected = pl.DataFrame(
                {
                    "literal": [
                        None,
                        1,
                        1.5,
                        2.25,
                        2.25,
                        2.25,
                    ]
                }
            )
    elif ignore_nulls & ~start_null:
        expected = pl.DataFrame(
            {
                "literal": [
                    0.,
                    0.5,
                    1.25,
                    2.125,
                    2.125,
                    2.125,
                ]
            }
        )
    elif ~ignore_nulls & start_null:
        expected = pl.DataFrame(
            {
                "literal": [
                    None, 
                    1,
                    1.5,
                    2.25,
                    None,
                    None
                ]
            }
        ).with_columns(pl.col("literal").cast(pl.Float64))
    else:
        expected = pl.DataFrame(
            {
                "literal": [
                    0.0,
                    0.5,
                    1.25,
                    2.125,
                    None,
                    None
                ]
            }
        )
        
    assert_frame_equal(result, expected)