
SHELL=/bin/bash

venv:  ## Set up virtual environment
	python3 -m venv venv
	venv/bin/pip install -r requirements.txt

install: venv
	unset CONDA_PREFIX && \
	source venv/bin/activate && maturin develop

install-release: venv
	unset CONDA_PREFIX && \
	source venv/bin/activate && maturin develop --release

pre-commit: venv
	cargo fmt --all && cargo clippy --all-features

run: install
	source venv/bin/activate && python run.py

run-release: install-release
	source venv/bin/activate && python run.py