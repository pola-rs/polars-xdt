import polars as pl
import polars_business as plb
from datetime import date

weekend = ["Sat", "Sun"]
holidays = [date(2000, 1, 1)]

df = pl.DataFrame(
    {
        "start_date": [date(2000, 3, 1), date(2000, 4, 3)],
        "end_date": [date(2000, 3, 3), date(2000, 4, 19)],
    }
)

print(
    df.with_columns(
        start_plus_3bd=plb.col("start_date").bdt.offset_by(
            "3bd", weekend=weekend, holidays=holidays
        ),
        start_is_workday=plb.col("start_date").bdt.is_workday(
            weekend=weekend, holidays=holidays
        ),
        workday_count=plb.workday_count(
            "start_date",
            "end_date",
            weekend=weekend,
            holidays=holidays,
        ),
    )
)
