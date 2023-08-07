# FOCUS (FinOps Open Source Cost and Usage Specification) Validator

Validator resource for checking datasets against the [FOCUS](https://focus.finops.org) specification.

## Overview

tbd

## Environment Setup

### Prerequisites

- Python 3.8+
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

If you have tests for your project, you can run them with:

```bash
poetry run pytest
```

Ensure you have `pytest` defined as a development dependency in your `pyproject.toml`.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
