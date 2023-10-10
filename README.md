# WIP

Just sharing this try debugging something

Steps to reproduce:

1. make venv: `python3.11 -m venv .venv`
2. `.venv/bin/activate`
3. `pip install maturin polars`
4. `cd src`
5. `maturin develop -m expression_lib/Cargo.toml `
6. `python run.py`
