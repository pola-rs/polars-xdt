import polars as pl
from polars_business import *
from datetime import date, datetime, timedelta
import numpy as np

start = date(2000, 8, 1)
n = -29
holidays = [date(2000, 8, 1)]
df = pl.DataFrame(
    {
        "dates": pl.date_range(start, start+timedelta(10), eager=True),
    }
)
df = df.filter((~pl.col("dates").dt.weekday().is_in([5,6,7])))# & ~pl.col("dates").is_in(holidays))
df = df.with_columns(start_wday=pl.col("dates").dt.strftime("%a"))

print(
    df.with_columns(
        dates_shifted=pl.col("dates").business.advance_n_days(
            n=n,
            holidays=holidays,
        )
    ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
)
print(
    df.with_columns(
        dates_shifted=pl.Series(
            np.busday_offset(
                df["dates"],
                n,
                holidays=holidays,
                weekmask='1111001',
            )
        )
    )
)
