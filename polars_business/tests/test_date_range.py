import polars as pl
import pytest
from datetime import date
import polars_business as plb
from polars.testing import assert_series_equal


def test_eager() -> None:
    result = plb.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True)
    expected = pl.Series('date', [date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5), date(2023, 1, 6), date(2023, 1, 9), date(2023, 1, 10)])
    assert_series_equal(result, expected)

def test_expr() -> None:
    expected = pl.Series('date', [date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5), date(2023, 1, 6), date(2023, 1, 9), date(2023, 1, 10)])
    result = pl.select(plb.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True))['date']
    assert_series_equal(result, expected)

def test_invalid() -> None:
    with pytest.raises(ValueError):
        plb.date_range(date(2023, 1, 1), date(2023, 1, 10), '1bd1h')

def test_eager_custom_weekend() -> None:
    result = plb.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True, weekend=['Fri', 'Sat'])
    expected = pl.Series('date', [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5), date(2023, 1, 8), date(2023, 1, 9), date(2023, 1, 10)])
    assert_series_equal(result, expected)

def test_eager_custom_holiday() -> None:
    result = plb.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True, weekend=['Fri', 'Sat'], holidays=[date(2023, 1, 2)])
    expected = pl.Series('date', [date(2023, 1, 1), date(2023, 1, 3), date(2023, 1, 4), date(2023, 1, 5), date(2023, 1, 8), date(2023, 1, 9), date(2023, 1, 10)])
    assert_series_equal(result, expected)
