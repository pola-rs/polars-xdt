# Polars-business

<h1 align="center">
	<img
		width="400"
		alt="polars-business"
		src="https://github.com/MarcoGorelli/polars-business/blob/d38b5a68ae7aa8d5bacacb16359dc851f2c1e637/assets/polars-business.png">
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
- `plb.date_range`: just like [polars.date_range](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.date_range.html#polars-date-range),
  but also accepts:
  - `'1bd'` in the string language (i.e. "1 business day")
  - `holidays` argument, for passing custom holidays
  - `weekend` argument, for passing custom a weekend (default is ('Sat', 'Sun'))
- `plb.datetime_range`: same as above, but the output will be `Datetime` dtype.

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
    date_shifted=plb.col("date").bdt.offset_by('5bd')
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
    date_shifted=plb.col("date").bdt.advance_n_days(
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
    date_shifted=plb.col("date").bdt.offset_by(
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

Benchmarks
----------

Single-threaded performance is:
- about on par with NumPy
- at least an order of magnitude faster than pandas.

but note that Polars will take care of parallelisation for you.

Check out https://www.kaggle.com/code/marcogorelli/polars-business for some comparisons.
