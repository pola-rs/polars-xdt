import polars as pl
from polars.testing import assert_frame_equal
import polars_xdt as xdt
import pytest
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
        pl.when(when).then(float("NaN")).otherwise(pl.col("values")), times="times", half_life=timedelta(days=1), ignore_nulls=ignore_nulls
    ))
    
    if ignore_nulls:
        if start_null:
            expected = pl.DataFrame(
                {
                    "literal": [
                        float("NaN"),
                        1,
                        1.5,
                        2.25,
                        2.25,
                        2.25,
                    ]
                }
            )
        else:
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
    elif start_null:
        expected = pl.DataFrame(
            {
                "literal": n*[float("NaN")]
            }
        )
    else:
        expected = pl.DataFrame(
            {
                "literal": [
                    0.0,
                    0.5,
                    1.25,
                    2.125,
                    float("NaN"),
                    float("NaN")
                ]
            }
        )
        
    assert_frame_equal(result, expected)