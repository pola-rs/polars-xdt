import polars as pl
import numpy as np
import polars_business as plb
from datetime import date

df = pl.DataFrame(
    {
        "start": pl.date_range(date(2019, 12, 30), date(2020, 2, 8), eager=True),
        "end": [date(2020, 2, 3)] * 41,
    }
)
with pl.Config(tbl_rows=100):
    print(
        df.with_columns(
            start_weekday=pl.col("start").dt.weekday(),
            end_weekday=pl.col("end").dt.weekday(),
            result=plb.col("end").bdt.sub("start", weekend=('Sat', 'Sun')),
            result_np=pl.Series(np.busday_count(df["start"], df["end"])),
        )
    )
