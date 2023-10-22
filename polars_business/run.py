import polars as pl
import polars_business as plb
from datetime import date, datetime, timedelta
import numpy as np
from typing import Sequence, Iterable

reverse_mapping = {value: key for key, value in plb.mapping.items()}

start = date(2998, 1, 10)
n = 0
weekend = ["Sat", "Sun"]
holidays = []  # type: ignore
weekmask = [0 if reverse_mapping[i] in weekend else 1 for i in range(1, 8)]

df = pl.DataFrame({"dates": [start]})
df = df.with_columns(start_wday=pl.col("dates").dt.strftime("%a"))

print(
    df.with_columns(
        dates_shifted=plb.col("dates").bdt.offset_by(
            by=f"{n}bd",
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

print("here")
print(pl.select(plb.date_range(date(2020, 1, 1), date(2020, 2, 1))))
print("there")
print(plb.date_range(date(2020, 1, 1), date(2020, 2, 1), '2bd1h', eager=True))
