import polars as pl
from expression_lib import Language
from datetime import date, datetime

df = pl.DataFrame({
    "names": ["Richard", "Alice", "Bob"],
    "dates": [1]*3,
    "dates2": [date(2020, 1, 1)]*3,
})
print(df)

print(df.with_columns(dates_plus_1=pl.col('dates2').language.add_bday()))
