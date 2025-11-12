import importlib.resources
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import sqlglot

from focus_validator.data_loaders import data_loader
from focus_validator.outputter.outputter import Outputter
from focus_validator.rules.spec_rules import SpecRules, ValidationResults
from focus_validator.utils.performance_logging import logPerformance

DEFAULT_VERSION_SETS_PATH = str(
    importlib.resources.files("focus_validator").joinpath("rules")
)


class Validator:
    def __init__(
        self,
        data_filename: Optional[str],
        output_destination: Optional[str],
        output_type: str,
        data_format: Optional[str] = None,
        rule_set_path: str = DEFAULT_VERSION_SETS_PATH,
        focus_dataset: Optional[str] = None,
        filter_rules: Optional[str] = None,
        rules_file_prefix: str = "model-",
        rules_version: Optional[str] = None,
        rules_file_suffix: str = ".json",
        rules_force_remote_download: bool = False,
        rules_block_remote_download: bool = False,
        allow_draft_releases: bool = False,
        allow_prerelease_releases: bool = False,
        column_namespace: Optional[str] = None,
        applicability_criteria: Optional[str] = None,
        explain_mode: bool = False,
        transpile_dialect: Optional[str] = None,
        show_violations: bool = False,
    ) -> None:
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.data_filename = data_filename
        self.data_format = data_format
        self.focus_data = None
        self.data_row_count = 0  # Will be set during data loading
        self.focus_dataset = focus_dataset
        self.explain_mode = explain_mode
        self.transpile_dialect = transpile_dialect
        self.show_violations = show_violations

        # Log validator initialization
        self.log.info("Initializing FOCUS Validator")
        self.log.debug("Data file: %s", data_filename)
        self.log.debug("Rule set path: %s", rule_set_path)
        self.log.debug("Rules version: %s", rules_version)
        self.log.debug("Focus dataset: %s", focus_dataset)
        self.log.debug(
            "Output type: %s, destination: %s", output_type, output_destination
        )
        if explain_mode:
            self.log.info(
                "Explain mode enabled - will generate SQL explanations without validation"
            )
            if transpile_dialect:
                self.log.info(
                    "SQL transpilation enabled for dialect: %s", transpile_dialect
                )

        if filter_rules:
            self.log.info("Rule filtering enabled: %s", filter_rules)
        if column_namespace:
            self.log.info("Column namespace: %s", column_namespace)
        if rules_force_remote_download:
            self.log.info("Force remote download enabled")

        # Store original criteria string for processing after SpecRules creation
        self._original_applicability_criteria = applicability_criteria
        self.applicability_criteria_list = None

        self.rules_version = rules_version
        self.spec_rules = SpecRules(
            rule_set_path=rule_set_path,
            rules_file_prefix=rules_file_prefix,
            rules_version=self.rules_version,
            rules_file_suffix=rules_file_suffix,
            focus_dataset=focus_dataset,
            filter_rules=filter_rules,
            rules_force_remote_download=rules_force_remote_download,
            rules_block_remote_download=rules_block_remote_download,  # New parameter, defaulting to False
            allow_draft_releases=allow_draft_releases,
            allow_prerelease_releases=allow_prerelease_releases,
            column_namespace=column_namespace,
            applicability_criteria_list=None,  # Will be set later
            transpile_dialect=self.transpile_dialect,
        )

        # Process applicability criteria after SpecRules is created
        if self._original_applicability_criteria:
            if self._original_applicability_criteria.strip().upper() == "ALL":
                # Load all available criteria from the JSON file
                try:
                    all_criteria = self.get_applicability_criteria()
                    self.applicability_criteria_list = list(all_criteria.keys())
                    self.log.info(
                        "Using ALL applicability criteria (%d total): %s",
                        len(self.applicability_criteria_list),
                        self.applicability_criteria_list,
                    )
                except Exception as e:
                    self.log.warning(
                        "Failed to load all applicability criteria: %s. Proceeding with empty list.",
                        str(e),
                    )
                    self.applicability_criteria_list = []
            else:
                self.applicability_criteria_list = [
                    criteria.strip()
                    for criteria in self._original_applicability_criteria.split(",")
                    if criteria.strip()
                ]
                self.log.info(
                    "Applicability criteria filter: %s",
                    self.applicability_criteria_list,
                )

            # Update the SpecRules with the processed criteria
            self.spec_rules.applicability_criteria_list = (
                self.applicability_criteria_list
            )
        self.outputter = Outputter(
            output_type=output_type,
            output_destination=output_destination,
            show_violations=show_violations,
            focus_dataset=self.focus_dataset,
        )

        self.log.debug("Validator initialization completed")

    def get_spec_rules_path(self) -> str:
        return self.spec_rules.get_spec_rules_path()

    @logPerformance("validator.load", includeArgs=True)
    def load(self) -> None:
        self.log.info("Loading validation data and rules...")

        # Load rules first to extract column type information
        self.log.debug("Loading specification rules...")
        self.spec_rules.load()

        # Skip data loading in explain mode
        if self.explain_mode:
            self.log.info("Explain mode: skipping data loading")
            self.focus_data = None
            return

        # Get column types from the loaded rules
        column_types = self.spec_rules.get_column_types()
        if column_types:
            self.log.info(
                "Extracted column types for %d columns: %s",
                len(column_types),
                column_types,
            )
        else:
            self.log.debug("No column type information found in rules")

        # Load data with column type information
        if self.data_filename == "-":
            self.log.info("Loading data from stdin...")
        else:
            self.log.debug("Loading data from: %s", self.data_filename)
            if self.data_filename and os.path.exists(self.data_filename):
                file_size = os.path.getsize(self.data_filename)
                self.log.info("Data file size: %.2f MB", file_size / 1024 / 1024)

        dataLoader = data_loader.DataLoader(
            data_filename=self.data_filename,
            data_format=self.data_format,
            column_types=column_types,
        )
        self.focus_data = dataLoader.load()

        if self.focus_data is not None:
            try:
                row_count = len(self.focus_data)
                self.data_row_count = row_count  # Store for use in validation results
                col_count = (
                    len(self.focus_data.columns)
                    if hasattr(self.focus_data, "columns")
                    else "unknown"
                )
                self.log.info(
                    "Data loaded successfully: %s rows, %s columns",
                    row_count,
                    col_count,
                )
                self.log.debug(
                    "Column names: %s",
                    (
                        list(self.focus_data.columns)
                        if hasattr(self.focus_data, "columns")
                        else "N/A"
                    ),
                )
            except Exception as e:
                self.log.warning("Could not determine data dimensions: %s", e)

        self.log.info("Data and rules loading completed")

    @logPerformance("validator.validate", includeArgs=True)
    def validate(self) -> ValidationResults:
        self.log.info("Starting validation process...")
        self.load()

        # Validate
        self.log.debug("Executing rule validation...")
        results = self.spec_rules.validate(
            self.focus_data,
            show_violations=self.show_violations,
            data_filename=self.data_filename or "unknown",
            data_row_count=self.data_row_count,
        )

        # Output results
        self.log.debug("Writing validation results...")
        self.outputter = self.outputter.write(results)

        self.log.info("Validation process completed")
        return results

    @logPerformance("validator.explain", includeArgs=True)
    def explain(self) -> Dict[str, Dict[str, str]]:
        """Generate SQL explanations for validation rules without executing validation.

        Returns:
            Dictionary mapping rule_id to explanation dict containing SQL and metadata
        """
        self.log.info("Starting explain mode - generating SQL explanations...")
        self.load()  # This will load rules but skip data in explain mode

        # Generate SQL explanations
        self.log.debug("Generating SQL explanations for all rules...")
        sql_map = self.spec_rules.explain()

        self.log.info("SQL explanation generation completed for %d rules", len(sql_map))
        return sql_map

    def print_sql_explanations(
        self, sql_map: Dict[str, Dict[str, Any]], verbose: bool = False
    ) -> None:
        """Print SQL explanations in a human-readable format.

        Args:
            sql_map: Dictionary from explain() method
            verbose: If True, show full SQL queries, otherwise truncate
        """
        dialect_info = ""
        if self.transpile_dialect:
            dialect_info = f" (transpiled to {self.transpile_dialect})"

        print(f"\n=== SQL Explanations for {len(sql_map)} rules{dialect_info} ===\n")

        # Sort rules alphabetically by rule_id
        for rule_id in sorted(sql_map.keys()):
            explanation = sql_map[rule_id]
            rule_type = explanation.get("type", "unknown")
            check_type = explanation.get("check_type", "unknown")
            generator = explanation.get("generator", "unknown")

            print(f"📋 {rule_id}")
            print(f"   Type: {rule_type}")
            if check_type != "unknown":
                print(f"   Check: {check_type}")
            if generator != "unknown":
                print(f"   Generator: {generator}")

            # Show MustSatisfy if present
            must_satisfy = explanation.get("must_satisfy")
            if must_satisfy:
                print(f"   MustSatisfy: {must_satisfy}")

            # Show condition if present
            condition = explanation.get("row_condition_sql")
            if condition:
                print(f"   Condition: {condition}")

            # Show SQL for leaf rules
            sql = explanation.get("sql")
            if sql and sql != "None":
                # Transpile SQL if target dialect is specified
                if self.transpile_dialect:
                    try:
                        transpiled_sql = self._transpile_sql(
                            sql, self.transpile_dialect
                        )
                        print(f"   SQL (transpiled to {self.transpile_dialect}):")
                        formatted_sql = self._format_sql_for_display(transpiled_sql)
                    except Exception as e:
                        print(
                            f"   SQL (transpilation to {self.transpile_dialect} failed: {e}):"
                        )
                        formatted_sql = self._format_sql_for_display(sql)
                else:
                    print("   SQL:")
                    formatted_sql = self._format_sql_for_display(sql)

                # Indent each line of the SQL
                for line in formatted_sql.split("\n"):
                    print(f"     {line}")

            # Show composite info
            children = explanation.get("children")
            if children:
                print(f"   Children: {len(children)} rules")
                # Always show children for CompositeANDRuleGenerator and CompositeORRuleGenerator
                if verbose or generator in [
                    "CompositeANDRuleGenerator",
                    "CompositeORRuleGenerator",
                ]:
                    for child in children:
                        if isinstance(child, dict):
                            child_id = child.get("rule_id", "unknown")
                            child_type = child.get("type", "unknown")
                            child_check = child.get("check_type", "")

                            # For reference type children, show the referenced rule instead of the parent rule_id
                            if child_type == "reference":
                                referenced_id = child.get("referenced", child_id)
                                if referenced_id and referenced_id != child_id:
                                    child_display_id = f"{child_id} -> {referenced_id}"
                                else:
                                    child_display_id = child_id
                            else:
                                child_display_id = child_id

                            if child_check and child_check != "unknown":
                                print(
                                    f"     - {child_display_id} ({child_type}, {child_check})"
                                )
                            else:
                                print(f"     - {child_display_id} ({child_type})")
                    # For other composite types in verbose mode, limit to first 3
                    if (
                        verbose
                        and generator
                        not in ["CompositeANDRuleGenerator", "CompositeORRuleGenerator"]
                        and len(children) > 3
                    ):
                        print(f"     ... and {len(children) - 3} more")

            print()  # Empty line between rules

    def _format_sql_for_display(self, sql: str) -> str:
        """Format SQL for better display readability."""
        if not sql:
            return sql

        import re

        # Clean up whitespace first
        formatted = re.sub(r"\s+", " ", sql.strip())

        # Add newlines before major SQL keywords
        major_keywords = [
            "WITH",
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP BY",
            "ORDER BY",
            "HAVING",
            "UNION",
        ]
        for keyword in major_keywords:
            # Add newline before keyword (but not at the start)
            pattern = r"(?<!^)\s+" + keyword + r"\b"
            formatted = re.sub(pattern, r"\n" + keyword, formatted, flags=re.IGNORECASE)

        # Split into lines and add basic indentation
        lines = [line.strip() for line in formatted.split("\n") if line.strip()]
        result_lines = []

        for line in lines:
            # Main clauses start at base level
            if re.match(
                r"^\s*(WITH|SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|UNION)\b",
                line,
                re.IGNORECASE,
            ):
                result_lines.append(line)
            else:
                # Everything else gets indented
                result_lines.append("  " + line)

        return "\n".join(result_lines)

    def get_supported_versions(self) -> Tuple[List[str], List[str]]:
        self.log.debug("Retrieving supported versions...")
        local_versions = self.spec_rules.supported_local_versions()
        remote_versions = self.spec_rules.supported_remote_versions()
        self.log.debug(
            "Found %d local versions, %d remote versions",
            len(local_versions),
            len(remote_versions),
        )
        return local_versions, remote_versions

    def get_applicability_criteria(self) -> Dict[str, str]:
        """Get available applicability criteria for the configured FOCUS version.

        Returns:
            Dict mapping criteria ID to description
        """
        self.log.debug(
            "Retrieving applicability criteria for version %s...", self.rules_version
        )

        # Get the JSON file path
        json_file_path = self.spec_rules.get_spec_rules_path()

        # Load the JSON data directly
        from focus_validator.config_objects.json_loader import JsonLoader

        model_data = JsonLoader.load_json_rules(json_file_path)

        # Extract applicability criteria
        applicability_criteria = model_data.get("ApplicabilityCriteria", {})

        if not applicability_criteria:
            raise ValueError(
                f"No applicability criteria found in FOCUS version {self.rules_version}"
            )

        self.log.debug("Found %d applicability criteria", len(applicability_criteria))
        return dict(applicability_criteria)

    def _transpile_sql(self, sql: str, target_dialect: str) -> str:
        """Transpile SQL from DuckDB dialect to target dialect using SQLGlot.

        Args:
            sql: SQL query string in DuckDB dialect
            target_dialect: Target SQL dialect (e.g., 'postgres', 'mysql', 'snowflake', 'bigquery')

        Returns:
            Transpiled SQL string

        Raises:
            Exception: If transpilation fails
        """
        try:
            # Validate target dialect
            supported_dialects = [
                "bigquery",
                "clickhouse",
                "databricks",
                "drill",
                "duckdb",
                "hive",
                "mysql",
                "oracle",
                "postgres",
                "presto",
                "redshift",
                "snowflake",
                "spark",
                "sqlite",
                "starrocks",
                "tableau",
                "teradata",
                "trino",
            ]

            if target_dialect.lower() not in supported_dialects:
                raise ValueError(
                    f"Unsupported dialect '{target_dialect}'. "
                    f"Supported dialects: {', '.join(supported_dialects)}"
                )

            # Transpile from DuckDB to target dialect
            transpiled = sqlglot.transpile(
                sql, read="duckdb", write=target_dialect.lower(), pretty=True
            )

            if not transpiled:
                raise ValueError("SQLGlot returned empty result")

            return transpiled[0]

        except Exception as e:
            self.log.warning(
                "Failed to transpile SQL to %s: %s", target_dialect, str(e)
            )
            # Re-raise to let caller handle the error
            raise
