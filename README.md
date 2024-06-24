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
- ✅ convert to and from multiple time zones
- ✅ format datetime in different locales
- ✅ convert to Julian Dates
- ✅ ~time-based EWMA~ (upstreamed to Polars itself)
- ✅ ~custom business-day arithmetic~ (upstreamed to Polars itself)

Installation
------------

First, you need to [install Polars](https://pola-rs.github.io/polars/user-guide/installation/).

Then, you'll need to install `polars-xdt`:
```console
pip install polars-xdt
```

Read the [documentation](https://marcogorelli.github.io/polars-xdt-docs/) for a more examples and functionality.

Basic Example
-------------
Say we start with
```python
from datetime import datetime

import polars as pl
import polars_xdt as xdt

df = pl.DataFrame(
    {
        "local_dt": [
            datetime(2020, 10, 10, 1),
            datetime(2020, 10, 10, 2),
            datetime(2020, 10, 9, 20),
        ],
        "timezone": [
            "Europe/London",
            "Africa/Kigali",
            "America/New_York",
        ],
    }
)
```

Let's localize each datetime to the given timezone and convert to
UTC, all in one step:

```python
result = df.with_columns(
    xdt.from_local_datetime(
        "local_dt", pl.col("timezone"), "UTC"
    ).alias("date")
)
print(result)
```
```
shape: (3, 3)
┌─────────────────────┬──────────────────┬─────────────────────────┐
│ local_dt            ┆ timezone         ┆ date                    │
│ ---                 ┆ ---              ┆ ---                     │
│ datetime[μs]        ┆ str              ┆ datetime[μs, UTC]       │
╞═════════════════════╪══════════════════╪═════════════════════════╡
│ 2020-10-10 01:00:00 ┆ Europe/London    ┆ 2020-10-10 00:00:00 UTC │
│ 2020-10-10 02:00:00 ┆ Africa/Kigali    ┆ 2020-10-10 00:00:00 UTC │
│ 2020-10-09 20:00:00 ┆ America/New_York ┆ 2020-10-10 00:00:00 UTC │
└─────────────────────┴──────────────────┴─────────────────────────┘
```

Read the [documentation](https://marcogorelli.github.io/polars-xdt-docs/) for more examples!

Logo
----

Thanks to [Olha Urdeichuk](https://www.fiverr.com/olhaurdeichuk) for the illustration.
