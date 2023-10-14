import polars as pl
from polars_business import *
from datetime import date, datetime, timedelta
import numpy as np

start = date(2000, 3, 20)
n = -1
holidays = [date(2000, 3, 17)]
df = pl.DataFrame(
    {
        "dates": [start]
    }
)
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
            )
        )
    ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
)
