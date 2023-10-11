import polars as pl
from expression_lib import BusinessDayTools
from datetime import date, datetime
import numpy as np

df = pl.DataFrame({
    "dates": pl.date_range(date(1, 1, 1), date(9999, 1, 1), eager=True),
})
df = df.filter(pl.col('dates').dt.weekday() <6)

print(df.with_columns(dates_shifted=pl.col('dates').bdt.advance_by_days(n=15))[20:28])
print(df.with_columns(dates_shifted=pl.Series(np.busday_offset(df['dates'], 15)))[20:28]) 

import pandas as pd
dfpd = df.to_pandas()
print((dfpd + pd.tseries.offsets.BusinessDay(15)).iloc[20:28])

# Let's try to "just publish"

# only accept:
# - date
# - a single offset
