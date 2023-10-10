import polars as pl
from expression_lib import Language

df = pl.DataFrame({
    "names": ["Richard", "Alice", "Bob"],
    "moons": ["full", "half", "red"],
    "dist_a": [[12, 32, 1], [], [1, -2]],
    "dist_b": [[-12, 1], [43], [876, -45, 9]],
    "floats": [5.6, -1245.8, 242.224]
})


out = df.with_columns(
   pig_latin = pl.col("names").language.pig_latinnify(),
   pig_latin_moons = pl.col("moons").language.pig_latinnify(),
)
print(out)
