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
2. use `.bdt` accessor on expressions

See `Examples` below!

Examples
--------
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
    date_shifted=pl.col("date").bdt.offset_by('5bd')
)
print(result)
```
```
shape: (3, 2)
┌────────────┬──────────────┐
│ date       ┆ date_shifted │
│ ---        ┆ ---          │
│ date       ┆ date         │
╞════════════╪══════════════╡
│ 2023-04-03 ┆ 2023-04-10   │
│ 2023-09-01 ┆ 2023-09-08   │
│ 2024-01-04 ┆ 2024-01-11   │
└────────────┴──────────────┘
```

Let's shift `Date` forwards by 5 days, excluding Saturday and Sunday and UK holidays
for 2023 and 2024:

```python
import holidays

uk_holidays = holidays.country_holidays("UK", years=[2023, 2024])

result = df.with_columns(
    date_shifted=pl.col("date").bdt.advance_n_days(
      by='5bd',
      holidays=uk_holidays,
    )
)
print(result)
```
```
shape: (3, 2)
┌────────────┬──────────────┐
│ date       ┆ date_shifted │
│ ---        ┆ ---          │
│ date       ┆ date         │
╞════════════╪══════════════╡
│ 2023-04-03 ┆ 2023-04-11   │
│ 2023-09-01 ┆ 2023-09-08   │
│ 2024-01-04 ┆ 2024-01-11   │
└────────────┴──────────────┘
```

Let's shift `Date` forwards by 5 days, excluding only Sunday:
```python
result = df.with_columns(
    date_shifted=pl.col("date").bdt.offset_by(
      by='5bd',
      weekend=['Sun'],
    )
)
print(result)
```
```
shape: (3, 2)
┌────────────┬──────────────┐
│ date       ┆ date_shifted │
│ ---        ┆ ---          │
│ date       ┆ date         │
╞════════════╪══════════════╡
│ 2023-04-03 ┆ 2023-04-08   │
│ 2023-09-01 ┆ 2023-09-07   │
│ 2024-01-04 ┆ 2024-01-10   │
└────────────┴──────────────┘
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

Note that this is single threaded performance. In common usage these will likely run in parallel.

The following timings can be verified using the `perf.py` script (note: lower is better):

### Adding 17 business days to 1 million random dates (no holidays)

- Polars-business 0.00656s
- NumPy 0.00914s
- pandas 0.08006s

### Adding 17 business days to 1 million random dates (UK holidays for 2020-2023)

- Polars-business 0.03771s
- NumPy 0.04077s
- pandas: omitted as it's not vectorised and throws a `PerformanceWarning`

### Adding 17 business days to 1 million random dates (with 'Friday' and 'Saturday' as weekend)

- Polars-business 0.0108s
- NumPy 0.01057s
- pandas: omitted as it's not vectorised and throws a `PerformanceWarning`

### Adding 17 business days to 1 million random dates (with 'Friday' and 'Saturday' as weekend, and UK holidays for 2020-2023)

- Polars-business 0.0371s
- NumPy 0.03841s
- pandas: omitted as it's not vectorised and throws a `PerformanceWarning`
