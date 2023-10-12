
SHELL=/bin/bash

venv:  ## Set up virtual environment
	. ../.venv/bin/activate

install: venv
	unset CONDA_PREFIX && \
	source ../.venv/bin/activate && maturin develop -m polars_business/Cargo.toml

install-release: venv
	unset CONDA_PREFIX && \
	source ../.venv/bin/activate && maturin develop --release -m polars_business/Cargo.toml

build: venv
	unset CONDA_PREFIX && \
	source ../.venv/bin/activate && maturin develop --release -m polars_business/Cargo.toml

clean:
	-@rm -r ../.venv
	-@cd polars_business && cargo clean


run: install
	source ../.venv/bin/activate && python run.py

run-release: install-release
	source ../.venv/bin/activate && python run.py

test: build
	pytest ../tests

clippy: venv
	cargo clippy -p polars_business

fmt: venv
	cargo fmt --all
	black .

pre-commit: clippy fmt 
