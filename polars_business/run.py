import polars as pl
from polars_business import *
from datetime import date, datetime, timedelta
import numpy as np

reverse_mapping = {value: key for key, value in mapping.items()}

start = datetime(2000, 1, 18)
n = 11
weekend = ["Sat", "Sun"]
holidays = []  # [date(2000, 1, 29)]
weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(7)]

df = pl.DataFrame({"dates": [start]})
df = df.with_columns(start_wday=pl.col("dates").dt.strftime("%a"))

print(
    df.with_columns(
        dates_shifted=pl.col("dates").business.advance_n_days(
            n=n,
            holidays=holidays,
            weekend=weekend,
        )
    ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
)
print(
    df.with_columns(
        dates_shifted=pl.Series(
            np.busday_offset(
                df["dates"].dt.date(),
                n,
                holidays=holidays,
                weekmask=weekmask,
            )
        )
    ).with_columns(end_wday=pl.col("dates_shifted").dt.strftime("%a"))
)
