# Polars-business

<h1 align="center">
	<img
		width="400"
		alt="polars-business"
		src="https://github.com/MarcoGorelli/polars-business/assets/33491632/25fc2f26-8097-4b86-85c6-c40c75c30f38">
</h1>

[![PyPI version](https://badge.fury.io/py/polars-business.svg)](https://badge.fury.io/py/polars-business)

Business day utilities for [Polars](https://www.pola.rs/).

- ✅ blazingly fast, written in Rust!
- ✅ seamless Polars integration!
- ✅ define your own custom holidays and weekends!

Installation
------------

First, you need to [install Polars](https://pola-rs.github.io/polars/user-guide/installation/).

Then, you'll need to install `polars-business`:
```console
pip install polars-business
```

Then, if you can run
```python
from datetime import date
import polars_business as plb

print(plb.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True))
```
it means installation all worked correctly!

Usage
-----

1. `import polars_business as plb`
2. use `.bdt` accessor on expressions created via `plb.col`

Supported functions are:
- `Expr.bdt.offset_by`: just like [polars.Expr.dt.offset_by](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.Expr.dt.offset_by.html),
  but also accepts:
  - `'1bd'` in the string language (i.e. "1 business day")
  - `holidays` argument, for passing custom holidays
  - `weekend` argument, for passing custom a weekend (default is ('Sat', 'Sun'))
  - `roll` argument, for controlling what to do when the original date is not a workday
- `plb.date_range`, just like [polars.date_range](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.date_range.html#polars-date-range),
  but also accepts:
  - `'1bd'` in the string language (i.e. "1 business day")
  - `holidays` for passing custom holidays
  - `weekend` for passing custom a weekend (default is ('Sat', 'Sun'))
- `plb.workday_count`: count the  number of business dates between two `Date` columns!
  Arguments:
  - `start`: column with start dates
  - `end`: column with end dates
  - `holidays` for passing custom holidays
  - `weekend` for passing custom a weekend (default is ('Sat', 'Sun'))
- `Expr.bdt.is_workday`: determine if a given `Date` is a workday.
  Arguments:
  - `holidays` for passing custom holidays
  - `weekend` for passing custom a weekend (default is ('Sat', 'Sun'))

See `Examples` below!

Examples
--------
Say we start with
```python
from datetime import date

import polars as pl
import polars_business as plb


df = pl.DataFrame(
    {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
)
```

Let's shift `Date` forwards by 5 days, excluding Saturday and Sunday:

```python
result = df.with_columns(
    date_shifted=plb.col("date").bdt.offset_by(
      '5bd',
      weekend=('Sat', 'Sun'),
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
│ 2023-04-03 ┆ 2023-04-10   │
│ 2023-09-01 ┆ 2023-09-08   │
│ 2024-01-04 ┆ 2024-01-11   │
└────────────┴──────────────┘
```

Let's shift `Date` forwards by 5 days, excluding Friday, Saturday, and England holidays
for 2023 and 2024:

```python
import holidays

england_holidays = holidays.country_holidays("UK", subdiv='ENG', years=[2023, 2024])

result = df.with_columns(
    date_shifted=plb.col("date").bdt.offset_by(
    by='5bd',
    weekend=('Sat', 'Sun'),
    holidays=england_holidays,
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
│ 2023-04-03 ┆ 2023-04-12   │
│ 2023-09-01 ┆ 2023-09-08   │
│ 2024-01-04 ┆ 2024-01-11   │
└────────────┴──────────────┘
```

Count the number of business dates between two columns:
```python
df = pl.DataFrame(
    {
        "start": [date(2023, 1, 4), date(2023, 5, 1), date(2023, 9, 9)],
        "end": [date(2023, 2, 8), date(2023, 5, 2), date(2023, 12, 30)],
    }
)
result = df.with_columns(n_business_days=plb.workday_count('start', 'end'))
print(result)
```
```
shape: (3, 3)
┌────────────┬────────────┬─────────────────┐
│ start      ┆ end        ┆ n_business_days │
│ ---        ┆ ---        ┆ ---             │
│ date       ┆ date       ┆ i32             │
╞════════════╪════════════╪═════════════════╡
│ 2023-01-04 ┆ 2023-02-08 ┆ 25              │
│ 2023-05-01 ┆ 2023-05-02 ┆ 1               │
│ 2023-09-09 ┆ 2023-12-30 ┆ 80              │
└────────────┴────────────┴─────────────────┘
```

Benchmarks
----------

Single-threaded performance is:
- about on par with NumPy
- about an order of magnitude faster than pandas.

but note that Polars will take care of parallelisation for you, and that this plugin
will fit in with Polars lazy execution.

Check out https://www.kaggle.com/code/marcogorelli/polars-business for some comparisons.
