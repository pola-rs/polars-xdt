Installation
============

First, you need to [install Polars](https://pola-rs.github.io/polars/user-guide/installation/).

Then, you'll need to install `polars-business`:
```console
pip install polars-business
```

Then, if you can run
```python
from datetime import date
import polars_business as plb

print(plb.date_range(date(2023, 1, 1), date(2023, 1, 10), eager=True))
```
it means installation all worked correctly!
