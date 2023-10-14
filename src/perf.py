
import timeit
import numpy as np

# BENCHMARK 1: NO HOLIDAYS INVOLVED

setup = """
import polars as pl
import polars_business
from datetime import date
import numpy as np
import pandas as pd
import holidays
import warnings

dates = pl.date_range(date(2020, 1, 1), date(2024, 1, 1), closed='left', eager=True)
dates = dates.filter(dates.dt.weekday() < 6)
size = 10_000_000
input_dates = np.random.choice(dates, size)

df = pl.DataFrame({
    'ts': input_dates,
})

df_pd = pd.DataFrame({
    'ts': input_dates,
})
"""

def time_it(statement):
    results = np.array(timeit.Timer(
        stmt=statement,
        setup=setup,
        )
        .repeat(7, 3)
    )/3
    return round(min(results), 3)

print('Polars-business: ', time_it("result_pl = df.select(pl.col('ts').business.advance_n_days(n=17))"))

print('NumPy: ', time_it("result_np = np.busday_offset(input_dates, 17)"))

# uncomment, too slow...
# print('pandas: ', time_it("result_pd = df_pd['ts'] + pd.tseries.offsets.BusinessDay(17)"))

# BENCHMARK 2: WITH HOLIDAYS

setup = """
import polars as pl
import polars_business
from datetime import date
import numpy as np
import pandas as pd
import holidays
import warnings

uk_holidays = list(holidays.country_holidays('UK', years=[2020, 2021, 2022, 2023]))

dates = pl.date_range(date(2020, 1, 1), date(2024, 1, 1), closed='left', eager=True)
dates = dates.filter(~dates.is_in(uk_holidays))
dates = dates.filter(dates.dt.weekday() < 6)
size = 10_000_000
input_dates = np.random.choice(dates, size)

df = pl.DataFrame({
    'ts': input_dates,
})

df_pd = pd.DataFrame({
    'ts': input_dates,
})
"""

print('Polars-business: ', time_it("result_pl = df.select(pl.col('ts').business.advance_n_days(n=17, holidays=uk_holidays))"))

print('NumPy: ', time_it("result_np = np.busday_offset(input_dates, 17, holidays=uk_holidays)"))
