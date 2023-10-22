import polars as pl
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
