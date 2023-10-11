# Polars-business

![DALL·E 2023-10-11 16 02 10 - polar bear in business suit](https://github.com/MarcoGorelli/polars-business-day-tools/assets/33491632/46575111-4d14-452b-ac98-548acab3cf8f)

Business day utilities for [Polars](https://www.pola.rs/).

Install with `pip install polars-business-day-tools`.

Example
-------

Here's an example of how to shift a date range forwards by 5 business days (i.e. Monday to Friday, excluding weekends):
```python
import polars as pl
from polars_business_day_tools import BusinessDayTools
from datetime import date

df = pl.DataFrame({
    "dates": pl.date_range(date(2000, 1, 1), date(9999, 1, 1), eager=True),
})

print(df.with_columns(dates_shifted=pl.col('dates').bdt.advance_by_days(n=5)))
```
