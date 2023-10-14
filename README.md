# Polars-business

<h1 align="center">
	<img
		width="400"
		alt="polars-business"
		src="./assets/polars-business.png">
</h1>

Business day utilities for [Polars](https://www.pola.rs/).

Installation
------------

First, you need to install Polars. See the link above for how to do that.

Then, you'll need to install `polars-business`. Currently, you can do this via PyPI (note: the `$` is not part of the command):
```console
$ pip install polars-business
```

Usage
-----

Just `import polars_business` and then you'll get a `.business` accessor
on your expressions!

Currently there's only a single function: `advance_n_days`. It takes arguments:
- `n`: number of days to advance. This can be an expression.
- `holidays`: list of holidays in `datetime.date` format. The Python `holidays` package may
  be useful here. You can install it with `pip install holidays`, and then you can get a list
  of holidays for a given country with (for example, `'UK'`):
  ```python
  import holidays 

  list(holidays.country_holidays('UK', years=[2020, 2021, 2022, 2023]))
  ```

Example
-------

Given some dates, can you shift them all forwards by 5 business days (according to the UK holiday calendar)?

With `polars-business`, this is easy:
```python
from datetime import date

import holidays
import polars as pl
import polars_business


uk_holidays = holidays.country_holidays("UK", years=[2023, 2024])
df = pl.DataFrame(
    {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
)

result = df.with_columns(
    date_plus_5_business_days=pl.col("date").business.advance_n_days(
        n=5, holidays=uk_holidays
    )
)
print(result)
```
Output:
```console
shape: (3, 2)
┌────────────┬───────────────────────────┐
│ date       ┆ date_plus_5_business_days │
│ ---        ┆ ---                       │
│ date       ┆ date                      │
╞════════════╪═══════════════════════════╡
│ 2023-04-03 ┆ 2023-04-11                │
│ 2023-09-01 ┆ 2023-09-08                │
│ 2024-01-04 ┆ 2024-01-11                │
└────────────┴───────────────────────────┘
```

Note
----
Currently, only `pl.Date` datatype is supported.

What to expected
----------------
The following will hopefully come relatively soon:
- support for `Datetime`s
- support for rolling forwards/backwards to the next
  valid business date (if not already on one)

Ideas for future development:
- business date range
- support for custom week mask

Benchmarks
----------

The following timings can be verified using the `perf.py` script.

### Adding 17 business days to 10 million dates (no holidays)

- Polars-business 0.037
- NumPy 0.09
- pandas 0.801

### Adding 17 business days to 10 million dates (UK holidays for 2020-2023)

- Polars-business 0.34
- NumPy 0.403
- pandas: omitted as pandas doesn't (yet) vectorise `CustomBusinessDay`, so
  we'd likely be talking about minutes

### Adding 17 business days to 10 million dates (with 'Friday' and 'Saturday' as weekend)

- Polars-business 0.091
- NumPy 0.105
- pandas: omitted as pandas doesn't (yet) vectorise `CustomBusinessDay`, so
  we'd likely be talking about minutes
