.DEFAULT_GOAL := help
# Explicit targets to avoid conflict with files of the same name.
.PHONY: \
	requirements \
	lint check format \
	build test \
	help

PYTHONPATH=
SHELL=/usr/bin/env bash
VENV=.venv
UV_PYTHON ?= 3.12

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

.venv:  ## Set up a Python virtual environment and install dev packages
	UV_PYTHON=$(UV_PYTHON) uv venv $(VENV)

requirements: .venv ## Install/update Python dev packages
	@unset CONDA_PREFIX \
	&& uv pip install -e .[dev]

pytest: requirements
ifeq ($(OS),Windows_NT)
	set PYTHONPATH=./build
else
	export PYTHONPATH=./build
endif
	$(VENV_BIN)/python -m pytest -vv ./test

lint: requirements  ## Apply autoformatting and linting rules
	$(VENV_BIN)/ruff check src_py test
	$(VENV_BIN)/ruff format src_py test
	-$(VENV_BIN)/mypy src_py test

check: requirements
	$(VENV_BIN)/ruff check src_py test --verbose

format: requirements
	$(VENV_BIN)/ruff format src_py test

build:  ## Compile monad (and install in 'build') for Python
	$(MAKE) -C ../../ python
	cp src_py/*.py build/monad/

test: requirements  ## Run the Python unit tests
	cp src_py/*.py build/monad/ && cd build
	$(VENV_BIN)/pytest test

bench-neighbor-sampling: requirements ## Run synthetic neighbor sampling benchmark
	cp src_py/*.py build/monad/ && cd .. && $(VENV_BIN)/python bench/neighbor_sampling_bench.py

help:  ## Display this help information
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' | sort
