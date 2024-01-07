from __future__ import annotations
import pytest
import polars as pl
import polars_xdt as xdt
from datetime import datetime

from typing import TYPE_CHECKING
from polars.testing import assert_series_equal
from polars.exceptions import ComputeError
if TYPE_CHECKING:
    from polars.type_aliases import TimeUnit

@pytest.mark.parametrize("time_unit", ["ms", "us", "ns"])
def test_base_utc_offset(time_unit: TimeUnit) -> None:
    df = pl.datetime_range(
        datetime(2011, 12, 29),
        datetime(2012, 1, 1),
        "2d",
        time_zone="Pacific/Apia",
        eager=True,
    ).dt.cast_time_unit(time_unit).to_frame('a')
    result = df.select(xdt.col('a').xdt.base_utc_offset().alias("base_utc_offset"))['base_utc_offset']
    expected = pl.Series(
        "base_utc_offset",
        [-11 * 3600 * 1000, 13 * 3600 * 1000],
        dtype=pl.Duration("ms"),
    )
    assert_series_equal(result, expected)


def test_base_utc_offset_lazy_schema() -> None:
    ser = pl.datetime_range(
        datetime(2020, 10, 25),
        datetime(2020, 10, 26),
        time_zone="Europe/London",
        eager=True,
    )
    df = pl.DataFrame({"ts": ser}).lazy()
    result = df.with_columns(base_utc_offset=xdt.col("ts").xdt.base_utc_offset()).schema
    expected = {
        "ts": pl.Datetime(time_unit="us", time_zone="Europe/London"),
        "base_utc_offset": pl.Duration(time_unit="ms"),
    }
    assert result == expected


def test_base_utc_offset_invalid() -> None:
    df = pl.datetime_range(
        datetime(2011, 12, 29),
        datetime(2012, 1, 1),
        "2d",
        eager=True,
    ).to_frame('a')
    with pytest.raises(
        ComputeError,
        match=r"base_utc_offset only works on Datetime type",
    ):
        df.select(xdt.col('a').xdt.base_utc_offset())


@pytest.mark.parametrize("time_unit", ["ms", "us", "ns"])
def test_dst_offset(time_unit: TimeUnit) -> None:
    df = pl.datetime_range(
        datetime(2020, 10, 25),
        datetime(2020, 10, 26),
        time_zone="Europe/London",
        eager=True,
    ).dt.cast_time_unit(time_unit).to_frame('a')
    result = df.select(xdt.col('a').xdt.dst_offset().alias("dst_offset"))['dst_offset']
    expected = pl.Series("dst_offset", [3_600 * 1_000, 0], dtype=pl.Duration("ms"))
    assert_series_equal(result, expected)


def test_dst_offset_lazy_schema() -> None:
    ser = pl.datetime_range(
        datetime(2020, 10, 25),
        datetime(2020, 10, 26),
        time_zone="Europe/London",
        eager=True,
    )
    df = pl.DataFrame({"ts": ser}).lazy()
    result = df.with_columns(dst_offset=xdt.col("ts").xdt.dst_offset()).schema
    expected = {
        "ts": pl.Datetime(time_unit="us", time_zone="Europe/London"),
        "dst_offset": pl.Duration(time_unit="ms"),
    }
    assert result == expected


def test_dst_offset_invalid() -> None:
    df = pl.datetime_range(
        datetime(2011, 12, 29),
        datetime(2012, 1, 1),
        "2d",
        eager=True,
    ).to_frame('a')
    with pytest.raises(
        ComputeError,
        match=r"base_utc_offset only works on Datetime type",
    ):
        df.select(xdt.col('a').xdt.dst_offset())