import polars as pl
import polars_business as plb
from datetime import date

df = pl.DataFrame({"ts": [date(2020, 1, 1)]})

print(df.with_columns(ts_shifted=plb.col("ts").bdt.offset_by('3bd')))
