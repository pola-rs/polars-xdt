import polars as pl
from polars_business import *
from datetime import date, datetime
import numpy as np

df = pl.DataFrame({
    "dates": pl.date_range(date(2000, 1, 1), date(9999, 1, 1), eager=True),
})
df = df.filter(pl.col('dates').dt.weekday() <6)

print(df.head().with_columns(dates_shifted=pl.col('dates').business.advance_n_days(n=3))[:5])
print(df.head().with_columns(dates_shifted=pl.Series(np.busday_offset(df.head()['dates'], 3)))[:5]) 

import pandas as pd
dfpd = df.to_pandas()
print((dfpd + pd.tseries.offsets.BusinessDay(15)).iloc[20:28])

# Let's try to "just publish"

# only accept:
# - date
# - a single offset
