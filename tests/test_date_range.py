from __future__ import annotations
import polars as pl
import pytest
from datetime import date
import polars_xdt as xdt  # noqa: F401
from polars.testing import assert_series_equal


def test_eager() -> None:
    result = xdt.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True)
    expected = pl.Series(
        "literal",
        [
            date(2023, 1, 2),
            date(2023, 1, 3),
            date(2023, 1, 4),
            date(2023, 1, 5),
            date(2023, 1, 6),
            date(2023, 1, 9),
            date(2023, 1, 10),
        ],
    )
    assert_series_equal(result, expected)


def test_expr() -> None:
    expected = pl.Series(
        "literal",
        [
            date(2023, 1, 2),
            date(2023, 1, 3),
            date(2023, 1, 4),
            date(2023, 1, 5),
            date(2023, 1, 6),
            date(2023, 1, 9),
            date(2023, 1, 10),
        ],
    )
    result = pl.select(
        xdt.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True)
    )["literal"]
    assert_series_equal(result, expected)


def test_invalid() -> None:
    with pytest.raises(ValueError):
        xdt.date_range(date(2023, 1, 1), date(2023, 1, 10), "1bd1h")


def test_eager_custom_weekend() -> None:
    result = xdt.date_range(
        date(2023, 1, 1), date(2023, 1, 10), eager=True, weekend=["Fri", "Sat"]
    )
    expected = pl.Series(
        "literal",
        [
            date(2023, 1, 1),
            date(2023, 1, 2),
            date(2023, 1, 3),
            date(2023, 1, 4),
            date(2023, 1, 5),
            date(2023, 1, 8),
            date(2023, 1, 9),
            date(2023, 1, 10),
        ],
    )
    assert_series_equal(result, expected)


def test_eager_custom_holiday() -> None:
    result = xdt.date_range(
        date(2023, 1, 1),
        date(2023, 1, 10),
        eager=True,
        weekend=["Fri", "Sat"],
        holidays=[date(2023, 1, 2)],
    )
    expected = pl.Series(
        "literal",
        [
            date(2023, 1, 1),
            date(2023, 1, 3),
            date(2023, 1, 4),
            date(2023, 1, 5),
            date(2023, 1, 8),
            date(2023, 1, 9),
            date(2023, 1, 10),
        ],
    )
    assert_series_equal(result, expected)
