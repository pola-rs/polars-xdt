# Polars-business

<h1 align="center">
	<img
		width="400"
		alt="polars-business"
		src="./assets/polars-business.png">
</h1>

Business day utilities for [Polars](https://www.pola.rs/).

- ✅ blazingly fast, written in Rust!
- ✅ define your own custom holidays!
- ✅ define your own custom weekend days!
- ✅ works with Polars lazy execution!

Installation
------------

First, you need to [install Polars](https://pola-rs.github.io/polars/user-guide/installation/).

Then, you'll need to install `polars-business`:
```console
pip install polars-business
```

Usage
-----

1. `import polars_business`
2. use `.business` accessor on expressions

See `Examples` section below!

Currently there's only a single function: `advance_n_days`. It takes arguments:
- `n`: number of days to advance. This can be an expression.
- `holidays`: list of holidays in `datetime.date` format. The Python `holidays` package may
  be useful here. You can install it with `pip install holidays`, and then you can get a list
  of holidays for a given country with (for example, `'UK'`):
  ```python
  import holidays 

  pl.col('date').business.advance_n_days(
    n=n,
    holidays=list(holidays.country_holidays('UK', years=[2020, 2021, 2022, 2023])),
  )
  ```
- `weekend`. By default, Saturday and Sunday are considered "weekend". But you can customise
  this by passing, for example:
  ```python
  pl.col('date').business.advance_n_days(
    n=n,
    weekend=['Fri', 'Sat'],
  )
  ```

Example
-------
Say we start with
```python
from datetime import date

import polars as pl
import polars_business


df = pl.DataFrame(
    {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
)
```

Let's shift `Date` forwards by 5 days, excluding Saturday and Sunday:

```python
result = df.with_columns(
    date_shifted=pl.col("date").business.advance_n_days(n=5)
)
print(result)
```

Let's shift `Date` forwards by 5 days, excluding Saturday and Sunday and UK holidays
for 2023 and 2024:

```python
import holidays

uk_holidays = holidays.country_holidays("UK", years=[2023, 2024])

result = df.with_columns(
    date_shifted=pl.col("date").business.advance_n_days(
      n=5,
      holidays=uk_holidays,
    )
)
print(result)
```

Let's shift `Date` forwards by 5 days, excluding Friday and Saturday:
```python
result = df.with_columns(
    date_shifted=pl.col("date").business.advance_n_days(
      n=5,
      weekend=['Fri', 'Sat'],
    )
)
print(result)
```

What to expected
----------------
The following will hopefully come relatively soon:
- support for rolling forwards/backwards to the next
  valid business date (if not already on one)
- calculate the number of business days between two
  dates (like `np.busday_count`)

Ideas for future development:
- business date range

Benchmarks
----------

Note: take these with a grain of salt.

But I think they demonstrate:
- that `polars-business` is on-par with numpy for performance,
- that `polars-business` is at least an order of magnitude faster than pandas.

The following timings can be verified using the `perf.py` script.

### Adding 17 business days to 1 million random dates (no holidays)

- Polars-business 0.00656s
- NumPy 0.00914
- pandas 0.08006

### Adding 17 business days to 1 million random dates (UK holidays for 2020-2023)

- Polars-business 0.03771
- NumPy 0.04077
- pandas: omitted as it's not vectorised and throws a `PerformanceWarning`

### Adding 17 business days to 1 million random dates (with 'Friday' and 'Saturday' as weekend)

- Polars-business 0.0108
- NumPy 0.01057
- pandas: omitted as it's not vectorised and throws a `PerformanceWarning`

### Adding 17 business days to 1 million random dates (with 'Friday' and 'Saturday' as weekend, and UK holidays for 2020-2023)

- Polars-business 0.0371
- NumPy 0.03841
- pandas: omitted as it's not vectorised and throws a `PerformanceWarning`
