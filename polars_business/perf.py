# type: ignore
import timeit
import warnings
import numpy as np

BENCHMARKS = [1, 2, 3, 4]
# BENCHMARKS = [4]

SIZE = 1_000_000

# BENCHMARK 1: NO HOLIDAYS INVOLVED

setup = f"""
import polars as pl
import polars_business as plb
from datetime import date
import numpy as np
import pandas as pd
import holidays
import warnings

dates = pl.date_range(date(2020, 1, 1), date(2024, 1, 1), closed='left', eager=True)
dates = dates.filter(dates.dt.weekday() < 6)
size = {SIZE}
input_dates = np.random.choice(dates, size)

df = pl.DataFrame({{
    'ts': input_dates,
}})

df_pd = pd.DataFrame({{
    'ts': input_dates,
}})
"""


def time_it(statement):
    results = (
        np.array(
            timeit.Timer(
                stmt=statement,
                setup=setup,
            ).repeat(7, 3)
        )
        / 3
    )
    return round(min(results), 5)


if 1 in BENCHMARKS:
    print(
        "Polars-business: ",
        time_it("result_pl = df.select(plb.col('ts').bdt.offset_by(by='17bd'))"),
    )
    print("NumPy: ", time_it("result_np = np.busday_offset(input_dates, 17)"))

    print(
        "pandas: ",
        time_it("result_pd = df_pd['ts'] + pd.tseries.offsets.BusinessDay(17)"),
    )

# BENCHMARK 2: WITH HOLIDAYS

setup = f"""
import polars as pl
import polars_business as plb
from datetime import date
import numpy as np
import pandas as pd
import holidays
import warnings

uk_holidays = list(holidays.country_holidays('UK', years=[2020, 2021, 2022, 2023]))

dates = pl.date_range(date(2020, 1, 1), date(2024, 1, 1), closed='left', eager=True)
dates = dates.filter(~dates.is_in(uk_holidays))
dates = dates.filter(dates.dt.weekday() < 6)
size = {SIZE}
input_dates = np.random.choice(dates, size)

df = pl.DataFrame({{
    'ts': input_dates,
}})

df_pd = pd.DataFrame({{
    'ts': input_dates,
}})
"""

if 2 in BENCHMARKS:
    print(
        "Polars-business: ",
        time_it(
            "result_pl = df.select(plb.col('ts').bdt.offset_by(by='17bd', holidays=uk_holidays))"
        ),
    )
    print(
        "NumPy: ",
        time_it("result_np = np.busday_offset(input_dates, 17, holidays=uk_holidays)"),
    )
    # with warnings.catch_warnings():
    #     import pandas as pd
    #     warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
    #     print(
    #         "pandas: ", time_it("result_pd = df_pd['ts'] + pd.tseries.offsets.CustomBusinessDay(17, holidays=uk_holidays)")
    #     )

# BENCHMARK 3: WITH weekends

setup = f"""
import polars as pl
import polars_business as plb
from datetime import date
import numpy as np
import pandas as pd
import holidays
import warnings


dates = pl.date_range(date(2020, 1, 1), date(2024, 1, 1), closed='left', eager=True)
weekend = ['Fri', 'Sat']
dates = dates.filter(~dates.dt.weekday().is_in([5, 6]))
size = {SIZE}
input_dates = np.random.choice(dates, size)

df = pl.DataFrame({{
    'ts': input_dates,
}})

df_pd = pd.DataFrame({{
    'ts': input_dates,
}})
"""

if 3 in BENCHMARKS:
    print(
        "Polars-business: ",
        time_it(
            "result_pl = df.select(plb.col('ts').bdt.offset_by(by='17bd', weekend=weekend))"
        ),
    )
    print(
        "NumPy: ",
        time_it("result_np = np.busday_offset(input_dates, 17, weekmask='1111001')"),
    )


# BENCHMARK 4: WITH weekends and holidays

setup = f"""
import polars as pl
import polars_business as plb
from datetime import date
import numpy as np
import pandas as pd
import holidays
import warnings

uk_holidays = list(holidays.country_holidays('UK', years=[2020, 2021, 2022, 2023]))

dates = pl.date_range(date(2020, 1, 1), date(2024, 1, 1), closed='left', eager=True)
weekend = ['Fri', 'Sat']
dates = dates.filter((~dates.dt.weekday().is_in([5, 6])) & (~dates.is_in(uk_holidays)))
size = {SIZE}
input_dates = np.random.choice(dates, size)

df = pl.DataFrame({{
    'ts': input_dates,
}})

df_pd = pd.DataFrame({{
    'ts': input_dates,
}})
"""

if 4 in BENCHMARKS:
    print(
        "Polars-business: ",
        time_it(
            "result_pl = df.select(plb.col('ts').bdt.offset_by(by='17bd', weekend=weekend, holidays=uk_holidays))"
        ),
    )
    print(
        "NumPy: ",
        time_it(
            "result_np = np.busday_offset(input_dates, 17, weekmask='1111001', holidays=uk_holidays)"
        ),
    )
