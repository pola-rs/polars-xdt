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

To use it, you'll need to `import polars_business`, and then you'll be a `.business` accessor
on your expressions!

Currently there's only a single function: `advance_n_days`.

Example
-------

Here's an example of how to shift a date range forwards by 5 business days (i.e. Monday to Friday, excluding weekends):
```python
import polars as pl
import polars_business

from datetime import date

df = pl.DataFrame({
    "dates": pl.date_range(date(2000, 1, 1), date(9999, 1, 1), eager=True),
})
df = df.filter(pl.col('dates').dt.weekday() <6)

print(df.with_columns(dates_shifted=pl.col('dates').business.advance_n_days(n=5)))
```

Note
----
Currently, only `pl.Date` datatype is supported.

What to expected
----------------
The following will hopefully come relatively soon:
- support for `Datetime`s
- support for custom holiday calendars
- support for rolling forwards/backwards to the next
  valid business date (if not already on one)

Ideas for future development:
- business date range
- support for custom mask


Currently there's only a single function: `advance_n_days`.
