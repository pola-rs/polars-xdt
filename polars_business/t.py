import polars as pl
import numpy as np
import polars_business as plb
from datetime import date

weekend = ["Sat", "Sun"]
holidays = [date(2000, 1, 1)]

import polars as pl
import polars_business as plb
import datetime as dt

data = {"a": [1, 2, 2], "date": [
    dt.date(2022, 2, 1),
    dt.date(2023, 2, 1),
    dt.date(2023, 3, 1),
]}
df = pl.DataFrame(data)

print(df
    .group_by(['a'])
    .agg(
        minDate=plb.col.date.min().bdt.offset_by('-0bd'),
        maxDate=plb.col.date.min().bdt.offset_by('0bd')
    )
)
