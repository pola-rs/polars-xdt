# polars-xdt

## eXtra stuff for DateTimes

<h1 align="center">
	<img
		width="400"
		alt="polars-xdt"
		src="https://github.com/MarcoGorelli/polars-xdt/assets/33491632/928c68c4-4e71-45a7-bc89-14922c7ce61b">
</h1>

[![PyPI version](https://badge.fury.io/py/polars-xdt.svg)](https://badge.fury.io/py/polars-xdt)
[![Read the docs!](https://img.shields.io/badge/Read%20the%20docs!-coolgreen?style=flat&link=https://marcogorelli.github.io/polars-xdt-docs/)](https://marcogorelli.github.io/polars-xdt-docs/)

eXtra stuff for DateTimes in [Polars](https://www.pola.rs/).

- ✅ blazingly fast, written in Rust
- ✅ custom business-day arithmetic
- ✅ convert to and from multiple time zones
- ✅ format datetime in different locales
- ✅ convert to Julian Dates

Installation
------------

First, you need to [install Polars](https://pola-rs.github.io/polars/user-guide/installation/).

Then, you'll need to install `polars-xdt`:
```console
pip install polars-xdt
```

Read the [documentation](https://marcogorelli.github.io/polars-xdt-docs/) for a little tutorial and API reference.

Basic Example
-------------
Say we start with
```python
from datetime import date

import polars as pl
import polars_xdt as xdt


df = pl.DataFrame(
    {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
)
```

Let's shift `Date` forwards by 5 days, excluding Saturday and Sunday:

```python
result = df.with_columns(
    date_shifted=xdt.offset_by(
      'date',
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
Note that `polars-xdt` also registers a `xdt` namespace in the `Expression` class, so you
could equivalently write the above using `pl.col('date').xdt.offset_by('5bd')` (but note
that then type-checking would not recognise the `xdt` attribute).

Read the [documentation](https://marcogorelli.github.io/polars-xdt-docs/) for more examples!

Logo
----

Thanks to [Olha Urdeichuk](https://www.fiverr.com/olhaurdeichuk) for the illustration.
