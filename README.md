# FOCUS (FinOps Open Source Cost and Usage Specification) Validator

Validator resource for checking datasets against the [FOCUS](https://focus.finops.org) specification.

## Overview

The FOCUS Validator is a comprehensive Python application designed to validate cloud cost and usage datasets against the FinOps Foundation's FOCUS specification. It provides a robust validation framework that can process large datasets, execute complex validation rules, and generate detailed reports about compliance with FOCUS standards.

## Codebase Architecture

The FOCUS Validator follows a modular architecture with clear separation of concerns. The codebase is organized into several key components:

### Core Architecture Overview

```text
focus_validator/
├── main.py              # CLI entry point and application orchestration
├── validator.py         # Main validation orchestrator
├── config/              # Configuration files (logging, etc.)
├── config_objects/      # Core validation logic and rule definitions
├── data_loaders/        # Data input handling (CSV, Parquet)
├── outputter/          # Result output formatting (Console, XML, etc.)
├── rules/              # Rule definitions and validation execution
└── utils/              # Utility functions (performance, currency codes)
```

### 1. Application Entry Point (`main.py`)

The main module serves as the CLI interface and application orchestrator:

- **Argument Parsing**: Comprehensive command-line interface with support for various validation options
- **Logging Setup**: Flexible logging configuration with YAML/INI support and multiple fallback strategies  
- **Validation Orchestration**: Coordinates the entire validation workflow from data loading to result output
- **Visualization Support**: Optional generation of validation dependency graphs using Graphviz
- **Version Management**: Handles both local and remote FOCUS specification versions

**Key Features:**

- Support for applicability criteria filtering
- Remote specification downloading from GitHub
- Performance timing and logging
- Cross-platform file opening for visualizations

### 2. Validation Orchestrator (`validator.py`)

The `Validator` class is the central coordinator that manages the validation process:

- **Data Loading**: Delegates to appropriate data loaders based on file type
- **Rule Loading**: Manages FOCUS specification rule loading and version handling
- **Validation Execution**: Orchestrates rule execution against loaded datasets
- **Result Management**: Coordinates output formatting and result persistence
- **Performance Monitoring**: Integrated performance logging using decorators

**Key Responsibilities:**

- Version compatibility checking (local vs remote specifications)
- Applicability criteria processing and filtering
- Data and rule loading coordination
- Validation result aggregation and output

### 3. Config Objects (`config_objects/`)

This module contains the core validation logic and configuration management:

#### Rule Definition (`rule.py`)

- **ModelRule**: Pydantic model for FOCUS specification rules
- **ValidationCriteria**: Detailed validation requirements and conditions
- **CompositeCheck**: Support for complex AND/OR logic in rule dependencies
- **Status Management**: Rule lifecycle management (Active, Draft, etc.)

#### DuckDB Schema Conversion (`focus_to_duckdb_converter.py`)

The `FocusToDuckDBSchemaConverter` represents the most sophisticated component of the validation engine, implementing a comprehensive SQL code generation framework with advanced pattern matching, error recovery, and performance optimization:

**Internal Architecture & Core Components:**

**Check Generator Registry System:**

```python
CHECK_GENERATORS: Dict[str, Dict[str, Any]] = {
    "ColumnPresent": {"generator": ColumnPresentCheckGenerator, "factory": lambda args: "ColumnName"},
    "TypeString": {"generator": TypeStringCheckGenerator, "factory": lambda args: "ColumnName"},
    "FormatDateTime": {"generator": FormatDateTimeGenerator, "factory": lambda args: "ColumnName"},
    "CheckModelRule": {"generator": CheckModelRuleGenerator, "factory": lambda args: "ModelRuleId"},
    "AND": {"generator": CompositeANDRuleGenerator, "factory": lambda args: "Items"},
    "OR": {"generator": CompositeORRuleGenerator, "factory": lambda args: "Items"}
    # ... 20+ total generators
}
```

**Generator Architecture Patterns:**

- **Abstract Base Class**: `DuckDBCheckGenerator` defines the contract with `REQUIRED_KEYS`, `DEFAULTS`, and `FREEZE_PARAMS` class variables
- **Template Method Pattern**: Subclasses implement `generateSql()` for SQL generation and `getCheckType()` for type identification
- **Factory Pattern**: Registry system enables dynamic generator selection based on FOCUS rule function names
- **Parameter Validation**: Automatic validation of required parameters using Pydantic-style checking with `REQUIRED_KEYS`
- **Immutable Parameters**: `FREEZE_PARAMS` creates read-only parameter objects using `MappingProxyType` for thread safety

**SQL Generation Framework:**

**Template System Architecture:**

- **Dual Template Support**: Handles both `{table_name}` and `{{table_name}}` patterns for backward compatibility
- **Parameter Substitution**: Safe parameter injection using `_lit()` method with SQL injection prevention
- **Dynamic SQL Construction**: Runtime SQL assembly based on rule requirements and data types
- **Query Optimization**: Template caching and SQL query plan reuse for repeated validations

**`build_check()` - Advanced Check Construction Pipeline:**

**Multi-Phase Construction Process:**

```python
# Detailed build_check() workflow
1. Rule Type Analysis → Determine generator class from CHECK_GENERATORS registry
2. Applicability Assessment → Evaluate rule inclusion via _should_include_rule()
3. Parameter Extraction → Parse ValidationCriteria.requirement for generator parameters
4. Generator Instantiation → Create generator with validated parameters and context
5. SQL Generation → Execute generateSql() with template substitution
6. Check Object Assembly → Wrap in DuckDBColumnCheck with metadata and execution context
7. Nested Check Handling → Recursively process composite rule children
8. Performance Optimization → Cache compiled checks for reuse
```

**Technical Implementation Details:**

**Dynamic Rule Processing:**

- **Rule Type Detection**: Uses `rule.is_dynamic()` to identify rules requiring runtime data analysis
- **Skipped Check Generation**: Creates `SkippedDynamicCheck` objects for rules that cannot be statically validated
- **Metadata Preservation**: Maintains rule context and reasoning for debugging and reporting

**Applicability Filtering Engine:**

```python
def _should_include_rule(self, rule, parent_edges=None) -> bool:
    # Hierarchical applicability checking
    1. Check rule's own ApplicabilityCriteria against validated_applicability_criteria
    2. Traverse parent_edges to validate entire dependency chain
    3. Apply AND logic - rule included only if all criteria in chain are satisfied
    4. Generate SkippedNonApplicableCheck for filtered rules
```

**Composite Rule Architecture:**

- **Recursive Descent Parsing**: Processes ValidationCriteria.requirement.Items recursively for nested AND/OR structures
- **Child Check Generation**: Each composite rule item becomes a separate DuckDBColumnCheck object
- **Logic Handler Assignment**: Assigns `operator.and_` or `operator.or_` functions for result aggregation
- **Dependency Context Propagation**: Passes parent results and edge context down to child generators

**`run_check()` - High-Performance Execution Engine:**

**Multi-Level Execution Strategy:**

```python
# Comprehensive execution pipeline
1. Connection Validation → Ensure DuckDB connection is active and prepared
2. Check Type Dispatch → Route to appropriate execution path (skipped/leaf/composite)
3. SQL Template Resolution → Substitute table names and parameters safely
4. Query Execution → Execute with comprehensive error handling and recovery
5. Result Processing → Parse DuckDB results and format for aggregation
6. Error Analysis → Extract column names from error messages for intelligent reporting
7. Nested Aggregation → Process composite rule children and apply logic operators
8. Performance Metrics → Capture execution timing and resource usage
```

**Advanced Error Recovery System:**

**Missing Column Detection:**

```python
def _extract_missing_columns(err_msg: str) -> List[str]:
    # Multi-pattern column extraction from DuckDB error messages
    patterns = [
        r'Column with name ([A-Za-z0-9_"]+) does not exist',
        r'Binder Error: .*? column ([A-Za-z0-9_"]+)',
        r'"([A-Za-z0-9_]+)" not found'
    ]
    # Returns sorted list of missing column names for precise error reporting
```

**SQL Error Classification:**

- **Syntax Errors**: Template substitution failures and malformed SQL detection
- **Schema Mismatches**: Column type conflicts and missing table detection  
- **Data Validation Errors**: Constraint violations and format validation failures
- **Performance Issues**: Timeout detection and resource exhaustion handling

**Composite Rule Execution Engine:**

**Upstream Dependency Short-Circuiting:**

```python
# Advanced dependency failure handling
if upstream_failure_detected:
    1. Mark all child checks as failed with upstream reason
    2. Preserve failure context and dependency chain information
    3. Skip actual SQL execution to avoid cascading errors
    4. Generate detailed failure report with root cause analysis
```

**Result Aggregation Logic:**

- **AND Logic**: `all(child_results)` - requires all children to pass
- **OR Logic**: `any(child_results)` - requires at least one child to pass
- **Failure Propagation**: Maintains detailed failure context including which specific children failed
- **Performance Optimization**: Early termination for AND (first failure) and OR (first success) operations

**Advanced SQL Generation Patterns:**

**Type-Specific Generators:**

**ColumnPresentCheckGenerator:**

```sql
WITH col_check AS (
    SELECT COUNT(*) AS found
    FROM information_schema.columns
    WHERE table_name = '{table_name}' AND column_name = '{column_name}'
)
SELECT 
    CASE WHEN found = 0 THEN 1 ELSE 0 END AS violations,
    CASE WHEN found = 0 THEN '{error_message}' END AS error_message
FROM col_check
```

**TypeStringCheckGenerator:**

```sql
SELECT 
    COUNT(*) AS violations,
    '{column_name} must be string type' AS error_message
FROM {table_name}
WHERE {column_name} IS NOT NULL 
    AND typeof({column_name}) != 'VARCHAR'
```

**FormatDateTimeGenerator:**

```sql
WITH datetime_violations AS (
    SELECT COUNT(*) AS violations
    FROM {table_name}
    WHERE {column_name} IS NOT NULL 
        AND TRY_STRPTIME({column_name}, '{expected_format}') IS NULL
)
SELECT violations, 
    CASE WHEN violations > 0 
        THEN '{column_name} format violations: ' || violations || ' rows'
    END AS error_message
FROM datetime_violations
```

**Performance & Memory Optimization:**

**Connection Management:**

- **Connection Pooling**: Reuses DuckDB connections across multiple validations
- **Memory Monitoring**: Tracks memory usage for large dataset processing
- **Query Plan Caching**: DuckDB query plan reuse for repeated validation patterns
- **Parallel Execution**: Thread-safe generator design for concurrent validation

**Algorithmic Efficiency:**

- **Lazy Evaluation**: SQL queries only executed when results are needed
- **Batch Processing**: Groups similar validations for bulk execution
- **Result Streaming**: Processes large result sets without loading entire datasets into memory
- **Index Utilization**: Generates SQL that leverages DuckDB's columnar indices

**Extensibility Framework:**

**Custom Generator Development:**

```python
class CustomCheckGenerator(DuckDBCheckGenerator):
    REQUIRED_KEYS = {"ColumnName", "ThresholdValue"}
    DEFAULTS = {"TolerancePercent": 0.1}
    
    def generateSql(self) -> str:
        # Custom SQL generation logic
        return f"SELECT COUNT(*) FROM {{table_name}} WHERE {self.params.ColumnName} > {self.params.ThresholdValue}"
    
    def getCheckType(self) -> str:
        return "custom_threshold_check"
```

**Registry Integration:**

- **Dynamic Registration**: New generators can be added to CHECK_GENERATORS at runtime
- **Parameter Validation**: Automatic validation using REQUIRED_KEYS and type checking
- **Factory Function**: Consistent parameter extraction across all generator types
- **Metadata Preservation**: Full context and provenance tracking for custom validations

#### Rule Dependency Resolution (`rule_dependency_resolver.py`)

The `RuleDependencyResolver` is the most sophisticated component in the validation engine, implementing advanced graph algorithms to analyze complex rule interdependencies and optimize execution paths:

**Internal Architecture & Data Structures:**

The resolver maintains three core data structures for efficient dependency management:

- **`dependency_graph`**: `Dict[str, Set[str]]` mapping rule_id → {dependent_rule_ids} representing forward edges
- **`reverse_graph`**: `Dict[str, List[str]]` mapping rule_id → [rules_that_depend_on_this] for backward traversal
- **`in_degree`**: `Dict[str, int]` tracking prerequisite counts for Kahn's algorithm implementation

**`buildDependencyGraph()` - Advanced Dependency Analysis:**

**Algorithm Implementation:**

```python
# Pseudocode for dependency graph construction
1. Filter rules by target prefix (e.g., "BilledCost*")
2. Recursively collect transitive dependencies using BFS
3. Build bidirectional graph structures for O(1) lookups
4. Calculate in-degree counts for topological processing
5. Validate graph integrity and detect potential cycles
```

**Technical Details:**

- **Rule Filtering**: Supports prefix-based filtering (e.g., `target_rule_prefix="BilledCost"`) to process subsets of the full rule graph, essential for large FOCUS specifications with 200+ rules
- **Transitive Closure**: Uses `_collectAllDependencies()` with deque-based BFS to recursively discover all child dependencies, ensuring composite rules include their nested components even when they don't match the prefix filter
- **Graph Construction**: Builds forward and reverse adjacency lists simultaneously for O(1) dependency lookups during execution
- **Memory Optimization**: Uses `defaultdict(set)` and `defaultdict(list)` to minimize memory allocation overhead
- **Cycle Prevention**: Maintains processed sets to prevent infinite recursion during dependency discovery

**Composite Rule Propagation:**

- **Condition Inheritance**: Implements `_propagate_composite_conditions()` to push parent composite rule conditions down to child rules via private attributes
- **CheckModelRule Processing**: Analyzes ValidationCriteria.requirement.Items to identify referenced rules and propagate preconditions
- **Runtime Condition Evaluation**: Child rules inherit parent conditions and evaluate them dynamically during execution

**`getTopologicalOrder()` - Advanced Graph Algorithms:**

**Kahn's Algorithm Implementation:**

```python
# Enhanced Kahn's algorithm with cycle detection
1. Initialize in-degree counts from dependency graph
2. Queue all zero-degree nodes (no prerequisites)
3. Process nodes level by level, updating dependent in-degrees
4. Detect cycles when remaining nodes > 0 after processing
5. Handle circular dependencies by appending remaining nodes
```

**Cycle Detection & Analysis:**

- **Tarjan's SCC Algorithm**: Implements `_tarjan_scc()` for strongly connected component detection with O(V+E) complexity
- **Cycle Visualization**: `_export_scc_dot()` generates Graphviz DOT files for each strongly connected component, enabling visual debugging of complex cycles
- **Simple Cycle Reconstruction**: `_find_simple_cycle()` uses DFS with path tracking to identify specific circular dependency chains
- **Detailed Cycle Logging**: Provides comprehensive cycle analysis including component sizes, adjacency matrices, and example cycle paths

**Advanced Debugging & Instrumentation:**

**Graph Analytics:**

- **`_log_graph_snapshot()`**: Captures comprehensive graph metrics (node count, edge count, zero-degree nodes) with sampling
- **`_trace_node()`**: Implements bounded DFS to trace dependency chains up to configurable depth for debugging blocked nodes
- **`_dump_blockers()`**: Analyzes remaining nodes after topological sort to identify specific blocking dependencies

**Performance Monitoring:**

- **Edge Counting**: Tracks total dependency relationships for complexity analysis
- **Zero-Degree Analysis**: Identifies entry points (rules with no dependencies) for parallel execution opportunities
- **Blocking Analysis**: Detailed reporting of rules that cannot be scheduled due to unsatisfied prerequisites

**`build_plan_and_schedule()` - Execution Plan Optimization:**

**PlanBuilder Integration:**

```python
# Advanced execution planning workflow
1. Create PlanBuilder with filtered relevant rules
2. Build parent-preserving forest from entry points
3. Compile to layered ValidationPlan for parallel execution
4. Apply runtime context and edge predicates
5. Generate deterministic scheduling with tie-breaking
```

**Technical Implementation:**

- **Forest Construction**: Uses `builder.build_forest(roots)` to create parent-preserving execution trees that maintain rule relationships
- **Compilation Pipeline**: Transforms abstract plan graph into concrete ValidationPlan with index-based node references for O(1) lookups
- **Edge Context Propagation**: Maintains EdgeCtx objects throughout planning to preserve dependency reasoning and conditional activation
- **Execution Context**: Supports runtime `exec_ctx` parameter for dynamic rule filtering and conditional execution paths

**Memory & Performance Optimizations:**

**Data Structure Efficiency:**

- **Deque-Based Processing**: Uses `collections.deque` for BFS traversals to minimize memory reallocation
- **Set Operations**: Leverages Python set operations for O(1) membership testing and efficient union/intersection operations  
- **Processed Tracking**: Maintains processed sets to prevent redundant work during recursive dependency discovery

**Algorithmic Complexity:**

- **Graph Construction**: O(V + E) where V = rules, E = dependencies
- **Topological Sort**: O(V + E) with Kahn's algorithm
- **SCC Detection**: O(V + E) with Tarjan's algorithm
- **Memory Usage**: O(V + E) for graph storage with minimal overhead

**Applicability Criteria Integration:**

**Dynamic Rule Filtering:**

- **Criteria Validation**: Supports `validated_applicability_criteria` parameter for runtime rule inclusion/exclusion
- **Hierarchical Processing**: Always includes rules in dependency graph but marks them for potential skipping during execution
- **SkippedNonApplicableCheck Generation**: Defers actual filtering to execution phase via converter's applicability checking

#### Plan Building (`plan_builder.py`)

- **Execution Planning**: Creates layered execution plans optimized for parallel processing
- **Edge Context Management**: Tracks why dependencies exist with conditional activation predicates  
- **Topological Scheduling**: Implements Kahn's algorithm with deterministic tie-breaking for consistent execution order
- **Parent Preservation**: Maintains parent-child relationships throughout the planning process for result aggregation

#### JSON Loading (`json_loader.py`)

- **Specification Parsing**: Loads and parses FOCUS JSON rule definitions
- **Version Management**: Handles multiple FOCUS specification versions
- **Remote Downloading**: Supports fetching specifications from GitHub releases

### 4. Data Loaders (`data_loaders/`)

Extensible data loading framework supporting multiple file formats:

#### Base Data Loader (`data_loader.py`)

- **Format Detection**: Automatic detection of file formats based on extension
- **Performance Monitoring**: Integrated loading performance tracking
- **Error Handling**: Comprehensive error handling and logging
- **Memory Management**: Efficient handling of large datasets

#### Format-Specific Loaders

- **CSV Loader** (`csv_data_loader.py`): Optimized CSV parsing with configurable options
- **Parquet Loader** (`parquet_data_loader.py`): High-performance Parquet file processing

**Key Features:**

- Automatic file type detection
- Performance monitoring and logging
- Memory-efficient processing for large datasets
- Extensible architecture for additional formats

### 5. Validation Rules Engine (`rules/`)

The core validation execution engine:

#### Specification Rules (`spec_rules.py`)

- **Rule Loading**: Manages loading of FOCUS specification rules from JSON
- **Version Management**: Handles multiple FOCUS versions and compatibility
- **Validation Execution**: Orchestrates rule execution against datasets using DuckDB
- **Result Aggregation**: Collects and organizes validation results
- **Remote Specification Support**: Downloads and caches remote specifications

**Validation Process:**

1. Load FOCUS specification from JSON files or remote sources
2. Parse rules and build dependency graph
3. Convert rules to executable DuckDB SQL queries  
4. Execute validation queries against dataset
5. Aggregate results and generate comprehensive reports

### 6. Output Formatters (`outputter/`)

Flexible output system supporting multiple formats:

#### Base Outputter (`outputter.py`)

- **Format Selection**: Factory pattern for output format selection
- **Result Processing**: Standardized result processing and formatting

#### Format-Specific Outputters

- **Console Outputter** (`outputter_console.py`): Human-readable terminal output
- **XML/JUnit Outputter** (`outputter_unittest.py`): CI/CD compatible XML reports
- **Validation Graph Outputter** (`outputter_validation_graph.py`): Graphviz visualizations

### 7. Utility Functions (`utils/`)

Supporting utilities for specialized functionality:

- **Performance Logging** (`performance_logging.py`): Decorator-based performance monitoring
- **Currency Code Downloads** (`download_currency_codes.py`): Dynamic currency validation support

### Data Flow Architecture

The validation process follows this high-level data flow:

1. **Input Processing**: CLI arguments parsed and configuration loaded
2. **Data Loading**: Dataset loaded using appropriate data loader (CSV/Parquet)
3. **Rule Loading**: FOCUS specification rules loaded and parsed from JSON
4. **Plan Building**: Validation execution plan built considering rule dependencies
5. **SQL Generation**: Rules converted to optimized DuckDB SQL queries
6. **Validation Execution**: SQL queries executed against dataset using DuckDB engine
7. **Result Aggregation**: Validation results collected and organized
8. **Output Generation**: Results formatted and output using selected formatter
9. **Optional Visualization**: Dependency graph and results visualized using Graphviz

### Key Design Principles

1. **Modularity**: Clear separation of concerns with pluggable components
2. **Performance**: Optimized for large datasets using DuckDB and efficient algorithms
3. **Extensibility**: Easy to add new data formats, output formats, and validation rules
4. **Reliability**: Comprehensive error handling and logging throughout
5. **Standards Compliance**: Full adherence to FOCUS specification requirements
6. **Developer Experience**: Rich logging, performance monitoring, and debugging support

### Technology Stack

- **Core Language**: Python 3.9+ with type hints and modern language features
- **Data Processing**: DuckDB for high-performance SQL-based validation
- **Data Formats**: Pandas/Polars for CSV/Parquet processing
- **Configuration**: Pydantic for type-safe configuration management
- **CLI**: argparse for comprehensive command-line interface
- **Visualization**: Graphviz for validation dependency graphs
- **Testing**: pytest with comprehensive test coverage
- **Code Quality**: Black, isort, flake8, mypy for code formatting and quality

### Testing Architecture

The FOCUS Validator maintains high code quality through comprehensive testing:

#### Test Organization

- **Unit Tests**: Component-level testing for individual modules
- **Integration Tests**: End-to-end validation workflow testing  
- **Performance Tests**: Large dataset processing validation
- **Configuration Tests**: Rule loading and version compatibility testing

#### Test Coverage Strategy

- **Data Loaders**: 100% coverage ensuring reliable data ingestion
- **Config Objects**: 97%+ coverage for core validation logic
- **Outputters**: 85%+ coverage across all output formats
- **Overall Project**: 70% coverage with targeted improvement areas

#### Quality Assurance Tools

- **pytest**: Primary testing framework with fixture support
- **pytest-cov**: Coverage reporting and analysis
- **Black**: Automated code formatting
- **isort**: Import organization and sorting
- **flake8**: Code style and complexity checking  
- **mypy**: Static type checking and validation
- **pre-commit**: Git hook integration for quality checks

### Development Workflow

#### Local Development Setup

1. **Environment Management**: Poetry-based dependency management
2. **Code Quality**: Automated formatting and linting on commit
3. **Performance Monitoring**: Built-in performance logging and profiling
4. **Testing**: Makefile targets for module-specific and comprehensive testing

#### Continuous Integration

The project uses GitHub Actions for automated quality assurance:

- **Linting Pipeline**: mypy, black, isort, flake8 validation
- **Test Execution**: Full test suite with coverage reporting
- **Performance Validation**: Memory and execution time monitoring
- **Multi-Platform Testing**: Cross-platform compatibility verification

#### Code Organization Principles

- **Type Safety**: Comprehensive type hints throughout codebase
- **Error Handling**: Structured exception hierarchy with detailed logging
- **Performance Focus**: Optimized for large dataset processing
- **Extensibility**: Plugin architecture for new formats and outputs
- **Documentation**: Inline documentation and comprehensive README

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

Activate the virtual environment (See: [poetry-plugin-shell](https://github.com/python-poetry/poetry-plugin-shell)):

```bash
poetry shell
```

Validations can be run using cli application `focus-validator`.

For help and more options:

```bash
focus-validator --help
```

## Explain Mode

The FOCUS Validator includes a powerful **Explain Mode** that allows you to inspect validation rules and their underlying SQL logic without executing actual validation or requiring input data. This feature is invaluable for understanding FOCUS specification requirements, debugging validation logic, and learning how rules are implemented.

### What is Explain Mode?

Explain Mode generates comprehensive explanations for all validation rules in a FOCUS specification version, including:

- **Rule Metadata**: Rule ID, type, check method, and generator information
- **MustSatisfy Requirements**: The human-readable requirement that each rule validates
- **SQL Queries**: Complete, formatted SQL queries that implement the validation logic
- **Rule Relationships**: Hierarchy and dependencies between composite and child rules
- **Condition Logic**: Row-level conditions and filters applied during validation

### How to Use Explain Mode

#### Basic Usage

```bash
# Explain all rules for FOCUS 1.2 with CostAndUsage dataset
focus-validator --validate-version 1.2 --focus-dataset CostAndUsage --explain-mode
```

#### With Applicability Criteria

```bash
# Show all rules including those with specific applicability requirements
focus-validator --validate-version 1.2 --focus-dataset CostAndUsage --explain-mode --applicability-criteria ALL

# Show rules for specific applicability criteria
focus-validator --validate-version 1.2 --focus-dataset CostAndUsage --explain-mode --applicability-criteria "AVAILABILITY_ZONE_SUPPORTED,MULTIPLE_SUB_ACCOUNT_TYPES_SUPPORTED"
```

#### View Available Applicability Criteria

```bash
# List all applicability criteria for a FOCUS version
focus-validator --validate-version 1.2 --show-applicability-criteria
```

### Explain Mode Output Format

The output is organized alphabetically by rule ID and includes comprehensive information for each rule:

#### Example Output

```text
=== SQL Explanations for 578 rules ===

📋 AvailabilityZone-C-001-M
   Type: leaf
   Check: type_string
   Generator: TypeStringCheckGenerator
   MustSatisfy: AvailabilityZone MUST be of type String.
   SQL:
     WITH invalid AS (
       SELECT 1
       FROM focus_data
       WHERE AvailabilityZone IS NOT NULL AND typeof(AvailabilityZone) != 'VARCHAR'
     )
     SELECT COUNT(*) AS violations, 
            CASE WHEN COUNT(*) > 0 
                 THEN 'AvailabilityZone MUST be of type VARCHAR (string).' 
            END AS error_message
     FROM invalid

📋 BilledCost-C-000-M
   Type: composite
   Check: composite
   Generator: CompositeANDRuleGenerator
   MustSatisfy: The BilledCost column adheres to the following requirements:
   Children: 7 rules
     - BilledCost-C-000-M -> BilledCost-C-001-M (reference, model_rule_reference)
     - BilledCost-C-000-M -> BilledCost-C-002-M (reference, model_rule_reference)
     - BilledCost-C-000-M -> BilledCost-C-003-M (reference, model_rule_reference)
     - BilledCost-C-000-M -> BilledCost-C-004-M (reference, model_rule_reference)
     - BilledCost-C-000-M -> BilledCost-C-005-C (reference, model_rule_reference)
     - BilledCost-C-000-M -> BilledCost-C-006-M (reference, model_rule_reference)
     - BilledCost-C-000-M -> BilledCost-C-007-M (reference, model_rule_reference)

📋 RegionId-C-000-C
   Type: skipped
   Check: None
   Generator: None due to non-applicable rule
   MustSatisfy: RegionId is RECOMMENDED to be present in a FOCUS dataset when the provider supports deploying resources or services within regions.
```

#### Output Components Explained

**📋 Rule Header**: Rule ID in alphabetical order

**Type**:

- `leaf` - Individual validation rule with SQL query
- `composite` - Rule that combines multiple child rules (AND/OR logic)
- `reference` - Rule that references another rule's outcome
- `skipped` - Rule that cannot be executed (dynamic or non-applicable)

**Check**: The validation method used:

- `type_string` - Validates column data type is string
- `column_presence` - Validates column exists in dataset
- `format_datetime` - Validates datetime format compliance
- `composite` - Combines multiple child rule results
- `model_rule_reference` - References another rule's result

**Generator**: The code generator that creates the validation logic:

- `TypeStringCheckGenerator` - Generates SQL for type validation
- `ColumnPresentCheckGenerator` - Generates SQL for column existence
- `CompositeANDRuleGenerator` - Combines child rules with AND logic
- `CompositeORRuleGenerator` - Combines child rules with OR logic
- `None due to dynamic rule` - Rule requires runtime data analysis
- `None due to non-applicable rule` - Rule doesn't apply to current criteria

**MustSatisfy**: Human-readable description of what the rule validates, directly from the FOCUS specification

**SQL**: Complete, formatted SQL query that implements the validation (for leaf rules only)

**Children**: For composite rules, shows all child rules with their types and references

**Condition**: Row-level conditions applied during validation (when present)

### Rule Types and Categories

#### Leaf Rules

Individual validation rules that execute SQL queries against the dataset. These represent the core validation logic.

**Examples:**

- Column presence validation
- Data type checking  
- Format validation (dates, currencies, etc.)
- Value constraint checking
- Pattern matching validation

#### Composite Rules

Rules that combine multiple child rules using logical operators:

- **AND Rules** (`CompositeANDRuleGenerator`): All child rules must pass
- **OR Rules** (`CompositeORRuleGenerator`): At least one child rule must pass

#### Reference Rules

Rules that mirror the outcome of another rule without executing additional SQL.

#### Skipped Rules

Rules that cannot be executed for various reasons:

- **Dynamic Rules**: Require runtime data analysis to determine validation logic
- **Non-Applicable Rules**: Don't apply based on current applicability criteria
- **Missing Dependencies**: Reference unavailable components or data

### Understanding Applicability Criteria

Some FOCUS rules only apply under specific conditions (e.g., when a provider supports availability zones). The `--applicability-criteria` option controls which rules are included:

- **Default (none specified)**: Shows only universally applicable rules
- **`ALL`**: Shows all rules including those with specific requirements
- **Specific criteria**: Shows rules for particular provider capabilities

Use `--show-applicability-criteria` to see available criteria for a FOCUS version.

### SQL Query Analysis

The SQL queries in explain mode show exactly how each validation is implemented:

#### Common SQL Patterns

**Column Presence Check:**

```sql
WITH col_check AS (
    SELECT COUNT(*) AS found
    FROM information_schema.columns
    WHERE table_name = 'focus_data' AND column_name = 'ColumnName'
)
SELECT CASE WHEN found = 0 THEN 1 ELSE 0 END AS violations,
       CASE WHEN found = 0 THEN 'Column "ColumnName" MUST be present in the table.' END AS error_message
FROM col_check
```

**Type Validation:**

```sql
SELECT COUNT(*) AS violations,
       'ColumnName MUST be of type VARCHAR (string).' AS error_message
FROM focus_data
WHERE ColumnName IS NOT NULL AND typeof(ColumnName) != 'VARCHAR'
```

**Format Validation:**

```sql
WITH invalid AS (
    SELECT ColumnName::TEXT AS value
    FROM focus_data
    WHERE ColumnName IS NOT NULL 
      AND NOT (ColumnName::TEXT ~ '^[pattern]$')
)
SELECT COUNT(*) AS violations,
       CASE WHEN COUNT(*) > 0 THEN 'Format validation message' END AS error_message
FROM invalid
```

### Use Cases for Explain Mode

#### 1. Learning FOCUS Specification

- Understand what each rule validates
- See the relationship between rules
- Learn validation requirements for each column

#### 2. Debugging Validation Issues

- Inspect SQL logic for failing rules
- Understand why certain rules are skipped
- Analyze composite rule dependencies

#### 3. Implementation Reference

- Use SQL patterns for custom validation tools
- Understand FOCUS rule implementation details
- Reference validation logic for documentation

#### 4. Rule Development

- Validate new rule implementations
- Test rule logic without full datasets
- Debug rule generation and SQL creation

### Performance and Output Management

Explain mode is designed for fast execution since it doesn't process actual data:

- **Fast Execution**: No data loading or SQL execution against datasets
- **Complete Coverage**: Analyzes all rules in the specification (500+ rules for FOCUS 1.2)
- **Alphabetical Ordering**: Predictable rule ordering for easy navigation
- **Detailed Output**: Comprehensive information for each rule

The output can be extensive (500+ rules), so consider using shell tools for navigation:

```bash
# Search for specific rules
focus-validator --validate-version 1.2 --focus-dataset CostAndUsage --explain-mode | grep "BilledCost"

# Page through output
focus-validator --validate-version 1.2 --focus-dataset CostAndUsage --explain-mode | less

# Save to file for analysis
focus-validator --validate-version 1.2 --focus-dataset CostAndUsage --explain-mode > rules_explanation.txt
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
