# Polars-business

<h1 align="center">
	<img
		width="400"
		alt="polars-business"
		src="https://github.com/MarcoGorelli/polars-business/assets/33491632/a743c3bd-3653-4362-a6bf-0984b8873e20">
</h1>

Business day utilities for [Polars](https://www.pola.rs/).

Installation
------------

First, you need to install Polars. See the link above for how to do that.

Then, you'll need to install `polars-business`. Currently, you can do this via PyPI (note: the `$` is not part of the command):
```console
$ pip install polars-business
```

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

print(df.with_columns(dates_shifted=pl.col('dates').business.advance_n_days(n=5)))
```
