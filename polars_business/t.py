import polars as pl
import numpy as np
import polars_business as plb
from datetime import date

df = pl.DataFrame(
    {
        "start": [date(2020, 1, 1)],
        "end": [date(2020, 1, 5)],
    }
)
holidays = [date(2020, 1, 3), date(2020, 1, 5)]
with pl.Config(tbl_rows=100):
    print(
        df.with_columns(
            start_weekday=pl.col("start").dt.weekday(),
            end_weekday=pl.col("end").dt.weekday(),
            result=plb.col("end").bdt.sub("start", weekend=('Sat', 'Sun'), holidays=holidays),
            result_np=pl.Series(np.busday_count(df["start"], df["end"], holidays=holidays)),
        )
    )
