import polars as pl
import numpy as np
import polars_business as plb
from datetime import date

weekend = ['Sat', 'Sun']
holidays = [date(2000, 1, 1)]

df = pl.DataFrame({
    'date': [date(2000, 3, 1), date(2000, 4, 3)]
})

print(df.with_columns(
    busday_offset=plb.col("date").bdt.offset_by('3bd', weekend=weekend, holidays=holidays),
    is_busday=plb.col("date").bdt.is_workday(weekend=weekend, holidays=holidays),
    busday_count=plb.col('date').bdt.sub(
        pl.lit(date(2000, 1, 2)), 
        weekend=weekend, 
        holidays=holidays
    )
))

# ok this is obviously wrong...what's going on???
