import polars as pl
from polars_business import *
from datetime import date, datetime
import numpy as np

# df = pl.DataFrame({
#     "dates": pl.date_range(date(2000, 12, 26), date(2000, 12, 30), eager=True),
# })
# df = df.filter(pl.col('dates').dt.weekday() <6)

# print(df.with_columns(dates_shifted=pl.col('dates').business.advance_n_days(
#     n=3,
#     holidays=[date(2000, 12, 25)]
#     )))
# print(df.with_columns(dates_shifted=pl.Series(np.busday_offset(df['dates'], 3,
#                                                                       holidays=[date(2000, 12, 25)]
#                                                                       )))) 

# E           date=datetime.date(2000, 4, 24),
# E           n=5,
# E           holidays=[datetime.date(2000, 1, 1), datetime.date(2000, 5, 1)],

df = pl.DataFrame({'ts': date(2000, 4, 24)})
print(df.with_columns(pl.col('ts').business.advance_n_days(5, holidays=[date(2000, 1, 1), date(2000, 5, 1)])))