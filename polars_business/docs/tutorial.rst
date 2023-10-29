Tutorial
========

Say we start with
```python
from datetime import date

import polars as pl
import polars_business as plb


df = pl.DataFrame(
    {"date": [date(2023, 4, 3), date(2023, 9, 1), date(2024, 1, 4)]}
)
```

Let's shift `Date` forwards by 5 days, excluding Saturday and Sunday:

```python
result = df.with_columns(
    date_shifted=plb.col("date").bdt.offset_by(
      '5bd',
      weekend=('Sat', 'Sun'),
    )
)
print(result)
```
```
shape: (3, 2)
┌────────────┬──────────────┐
│ date       ┆ date_shifted │
│ ---        ┆ ---          │
│ date       ┆ date         │
╞════════════╪══════════════╡
│ 2023-04-03 ┆ 2023-04-10   │
│ 2023-09-01 ┆ 2023-09-08   │
│ 2024-01-04 ┆ 2024-01-11   │
└────────────┴──────────────┘
```

Let's shift `Date` forwards by 5 days, excluding Friday, Saturday, and England holidays
for 2023 and 2024:

```python
import holidays

uk_holidays = holidays.country_holidays("UK", subdiv='England', years=[2023, 2024])

result = df.with_columns(
    date_shifted=plb.col("date").bdt.offset_by(
      by='5bd',
      weekend=('Sat', 'Sun'),
      holidays=uk_holidays,
    )
)
print(result)
```
```
shape: (3, 2)
┌────────────┬──────────────┐
│ date       ┆ date_shifted │
│ ---        ┆ ---          │
│ date       ┆ date         │
╞════════════╪══════════════╡
│ 2023-04-03 ┆ 2023-04-12   │
│ 2023-09-01 ┆ 2023-09-08   │
│ 2024-01-04 ┆ 2024-01-11   │
└────────────┴──────────────┘
```

Count the number of business dates between two columns:
```python
df = pl.DataFrame(
    {
        "start": [date(2023, 1, 4), date(2023, 5, 1), date(2023, 9, 9)],
        "end": [date(2023, 2, 8), date(2023, 5, 2), date(2023, 12, 30)],
    }
)
result = df.with_columns(n_business_days=plb.workday_count('start', 'end'))
print(result)
```
```
shape: (3, 3)
┌────────────┬────────────┬─────────────────┐
│ start      ┆ end        ┆ n_business_days │
│ ---        ┆ ---        ┆ ---             │
│ date       ┆ date       ┆ i32             │
╞════════════╪════════════╪═════════════════╡
│ 2023-01-04 ┆ 2023-02-08 ┆ 25              │
│ 2023-05-01 ┆ 2023-05-02 ┆ 1               │
│ 2023-09-09 ┆ 2023-12-30 ┆ 80              │
└────────────┴────────────┴─────────────────┘
```
