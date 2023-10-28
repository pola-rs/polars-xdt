import polars as pl
import numpy as np
import polars_business as plb
from datetime import date

weekend = ["Sat", "Sun"]
holidays = [date(2000, 1, 1)]

df = pl.DataFrame(
    {
        "date": [date(2000, 3, 1), date(2000, 4, 3)],
        "date2": [date(2000, 3, 3), date(2000, 4, 19)],
    }
)

print(
    df.with_columns(
        busday_offset=plb.col("date").bdt.offset_by(
            "3bd", weekend=weekend, holidays=holidays
        ),
        is_busday=plb.col("date").bdt.is_workday(weekend=weekend, holidays=holidays),
        workday_count=plb.col("date2").bdt.sub(
            date(2000, 1, 2), weekend=weekend, holidays=holidays
        ),
        workday_count2=plb.workday_count(
            date(2000, 1, 2),
            "date2",
            weekend=weekend,
            holidays=holidays,
        ),
    )
)

# ok this is obviously wrong...what's going on???
