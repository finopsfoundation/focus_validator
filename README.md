# FOCUS (FinOps Open Source Cost and Usage Specification) Validator

Validator resource for checking datasets against the [FOCUS](https://focus.finops.org) specification.

## Overview

tbd

## Environment Setup

### Prerequisites

- Python 3.9+
- Poetry (Package & Dependency Manager)

### Installation

#### 1. Install Poetry

If you haven't installed Poetry yet, you can do it by running:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

For alternative installation methods or more information about Poetry, please refer to
the [official documentation](https://python-poetry.org/docs/).

#### 2. Clone the repository

```bash
git clone https://github.com/finopsfoundation/focus-spec-validator.git
cd focus-spec-validator
```

#### 3. Install dependencies

Using Poetry, you can install the project's dependencies with:

```bash
poetry install
```

## Usage

Activate the virtual environment:

```bash
poetry shell
```

Validations can be run using cli application `focus-validator`.

For help and more options:

```bash
focus-validator --help
```

## Running Tests

### Basic Testing

Run all tests:

```bash
poetry run pytest
```

Run tests with verbose output:

```bash
poetry run pytest -v
```

### Test Coverage

Generate coverage report:

```bash
# Terminal coverage report
make coverage

# HTML coverage report (opens in browser)
make coverage-html

# Comprehensive coverage report (HTML + XML + terminal)
make coverage-report
```

### Module-Specific Testing

Run tests for specific modules:

```bash
# Data loaders only
make test-data-loaders

# Config objects only  
make test-config-objects

# Output formatters only
make test-outputter
```

### Coverage by Module

```bash
# Coverage for data loaders (100% coverage)
make coverage-data-loaders

# Coverage for config objects (97%+ coverage)
make coverage-config-objects

# Coverage for outputters (85%+ coverage)
make coverage-outputter
```

#### Current Test Coverage: 70% overall (257 tests passing)

- **Data Loaders**: 100% coverage (50 tests)
- **Config Objects**: 97%+ coverage (120 tests)
- **Outputters**: 85%+ coverage (80 tests)
- **Core Components**: 89%+ coverage (7 tests)

See `COVERAGE_REPORT.md` for detailed coverage analysis and improvement recommendations.

If running on legacy CPUs and the tests crash on the polars library, run the following locally only:

```bash
poetry add polars-lts-cpu
```

This will align the polars execution with your system hardware. It should NOT be committed back into the repository.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
