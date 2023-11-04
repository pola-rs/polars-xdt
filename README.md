# Polars-business

<h1 align="center">
	<img
		width="400"
		alt="polars-business"
		src="https://github.com/MarcoGorelli/polars-business/assets/33491632/928c68c4-4e71-45a7-bc89-14922c7ce61b">
</h1>

[![PyPI version](https://badge.fury.io/py/polars-business.svg)](https://badge.fury.io/py/polars-business)
[![Read the docs!](https://img.shields.io/badge/Read%20the%20docs!-coolgreen?style=flat&link=https://marcogorelli.github.io/polars-business-docs/)](https://marcogorelli.github.io/polars-business-docs/)

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

Read the [documentation](https://marcogorelli.github.io/polars-business-docs/) for a little tutorial and API reference.

Basic Example
-------------
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

You can also count the number of business days between two given dates, specify a custom
calendar holiday, and create a date range excluding workdays.

Read the [documentation](https://marcogorelli.github.io/polars-business-docs/) for more examples!

Performance
-----------

Single-threaded performance is:
- about on par with NumPy (it's essentially a Rust reimplementation)
- about an order of magnitude faster than pandas.

but note that Polars will take care of parallelisation for you, and that this plugin
will fit in with Polars lazy execution and not require any data conversion.
