# Focus Validator Makefile

.PHONY: help test test-verbose coverage coverage-html coverage-report clean install lint format

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies using poetry
	poetry install

test: ## Run all tests
	poetry run pytest

test-verbose: ## Run all tests with verbose output
	poetry run pytest -v

coverage: ## Run tests with coverage
	poetry run pytest --cov=focus_validator --cov-report=term-missing

coverage-html: ## Generate HTML coverage report
	poetry run pytest --cov=focus_validator --cov-report=html --cov-report=term-missing
	@echo "HTML coverage report generated in htmlcov/index.html"

coverage-report: ## Generate comprehensive coverage report (HTML, XML, terminal)
	poetry run pytest --cov=focus_validator --cov-report=html --cov-report=xml --cov-report=term-missing
	@echo "Coverage reports generated:"
	@echo "  - HTML: htmlcov/index.html"
	@echo "  - XML: coverage.xml"

coverage-data-loaders: ## Run coverage for data_loaders module only
	poetry run pytest tests/data_loaders/ --cov=focus_validator.data_loaders --cov-report=term-missing

coverage-config-objects: ## Run coverage for config_objects module only
	poetry run pytest tests/config_objects/ --cov=focus_validator.config_objects --cov-report=term-missing

coverage-outputter: ## Run coverage for outputter module only
	poetry run pytest tests/outputter/ --cov=focus_validator.outputter --cov-report=term-missing

lint: ## Run linting
	poetry run flake8 focus_validator/
	poetry run mypy focus_validator/

format: ## Format code
	poetry run black focus_validator/
	poetry run isort focus_validator/

clean: ## Clean up generated files
	rm -rf htmlcov/
	rm -f coverage.xml
	rm -rf .coverage
	rm -rf .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test-data-loaders: ## Run data_loaders tests only
	poetry run pytest tests/data_loaders/ -v

test-config-objects: ## Run config_objects tests only
	poetry run pytest tests/config_objects/ -v

test-generators: ## Run DuckDB generator tests only
	poetry run pytest tests/config_objects/test_focus_to_duckdb_generators.py -v

coverage-generators: ## Run coverage for DuckDB generators specifically
	poetry run pytest tests/config_objects/test_focus_to_duckdb_generators.py --cov=focus_validator.config_objects.focus_to_duckdb_converter --cov-report=term-missing

test-outputter: ## Run outputter tests only
	poetry run pytest tests/outputter/ -v