# type: ignore
import timeit
import warnings
import numpy as np

BENCHMARKS = [1, 2, 3, 4]

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
size = {SIZE}
start_dates = np.random.choice(dates, size)
end_dates = np.random.choice(dates, size)

df = pl.DataFrame({{
    'start_date': start_dates,
    'end_date': end_dates,
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
        time_it("result_pl = df.select(plb.col('end_date').bdt.sub('start_date'))"),
    )
    print("NumPy: ", time_it("result_np = np.busday_count(start_dates, end_dates)"))
