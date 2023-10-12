import polars as pl
from polars_business import *
from datetime import date, datetime
import numpy as np
import holidays

hols = holidays.country_holidays("UK", years=[2020])

print(
    df.with_columns(
        pl.col("ts").business.advance_n_days(
            5, holidays=[date(2000, 1, 1), date(2000, 5, 1)]
        )
    )
)
