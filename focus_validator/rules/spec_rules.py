import os
import requests
import time
from typing import Dict, Any, Optional, List
import logging

import pandas as pd
import duckdb

from focus_validator.config_objects import (
    ChecklistObject,
    ChecklistObjectStatus,
    Rule,
    JsonLoader,
)
from focus_validator.config_objects.rule import InvalidRule
from focus_validator.config_objects.focus_to_duckdb_converter import (
    FocusToDuckDBSchemaConverter,
)
from focus_validator.exceptions import UnsupportedVersion, FailedDownloadError

log = logging.getLogger(__name__)

def convert_column_errors(df, checklist, condition_fn, match_fn):
    def process_row(row):
        if condition_fn(row):
            for check_name, check_obj in checklist.items():
                if match_fn(row, check_obj):
                    row["check"] = f"{check_name}:::{check_obj.friendly_name}"
                    row["column"] = check_obj.column_id
                    row["failure_case"] = None
                    return row
        return row

    return df.apply(process_row, axis=1)


def convert_missing_column_errors(df, checklist):
    return convert_column_errors(
        df,
        checklist,
        condition_fn=lambda row: (
            row["schema_context"] == "DataFrameSchema"
            and row["check"] == "column_in_dataframe"
        ),
        match_fn=lambda row, check_obj: (
            row["failure_case"] == check_obj.column_id
            and check_obj.rule_ref.check == "column_required"
        )
    )


def convert_dtype_column_errors(df, checklist):
    return convert_column_errors(
        df,
        checklist,
        condition_fn=lambda row: (
            row["schema_context"] == "Column"
            and row["check"].startswith("dtype")
        ),
        match_fn=lambda row, check_obj: row["column"] == check_obj.column_id
    )


def restructure_failure_cases_df(failure_cases: pd.DataFrame, checklist):
    failure_cases = convert_missing_column_errors(failure_cases, checklist)
    failure_cases = convert_dtype_column_errors(failure_cases, checklist)
    failure_cases = failure_cases.rename(
        columns={"column": "Column", "index": "Row #", "failure_case": "Values"}
    )

    failure_cases[["Check Name", "Description"]] = failure_cases["check"].str.split(
        ":::", expand=True
    )
    failure_cases = failure_cases.drop("check", axis=1)
    failure_cases = failure_cases.drop("check_number", axis=1)
    failure_cases = failure_cases.drop("schema_context", axis=1)

    failure_cases = failure_cases.rename_axis("#")
    failure_cases.index = failure_cases.index + 1

    failure_cases["Row #"] = failure_cases["Row #"] + 1
    failure_cases = failure_cases[
        ["Column", "Check Name", "Description", "Values", "Row #"]
    ]

    return failure_cases


class ValidationResult:
    log = logging.getLogger(__name__ + "." + __qualname__)
    checklist: Dict[str, ChecklistObject]
    failure_cases: Optional[pd.DataFrame]

    def __init__(
        self,
        checklist: Dict[str, ChecklistObject],
        failure_cases: Optional[pd.DataFrame] = None,
    ):
        self.__failure_cases__ = failure_cases
        self.__checklist__ = checklist

    def process_result(self):
        # For DuckDB validation, status is already set in the DuckDB converter
        # Just ensure any remaining pending items are marked as passed
        checklist = self.__checklist__
        self.failure_cases = self.__failure_cases__

        for check_list_object in checklist.values():
            if check_list_object.status == ChecklistObjectStatus.PENDING:
                check_list_object.status = ChecklistObjectStatus.PASSED
        self.checklist = checklist


class SpecRules:
    
    def __init__(
        self, rule_set_path, rules_file_prefix, rules_version, rules_file_suffix, focus_dataset, filter_rules, rules_force_remote_download, allow_draft_releases, allow_prerelease_releases, column_namespace,
    ):
        self.rule_set_path = rule_set_path
        self.rules_file_prefix = rules_file_prefix
        self.rules_version = rules_version
        self.rules_file_suffix = rules_file_suffix
        self.focus_dataset = focus_dataset
        self.filter_rules = filter_rules
        self.json_rule_file = os.path.join(
            self.rule_set_path, f"{self.rules_file_prefix}{self.rules_version}{self.rules_file_suffix}"
        )
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")

        self.log.info("Initializing SpecRules for version %s", rules_version)
        self.log.debug("Rule set path: %s", rule_set_path)
        self.log.debug("Focus dataset: %s", focus_dataset)
        self.log.debug("Rule file pattern: %s*%s", rules_file_prefix, rules_file_suffix)
        self.log.debug("Target rule file: %s", self.json_rule_file)

        self.rules_force_remote_download = rules_force_remote_download
        self.allow_draft_releases = allow_draft_releases
        self.allow_prerelease_releases = allow_prerelease_releases
        self.local_supported_versions = self.supported_local_versions()
        self.log.info("Found %d local supported versions: %s", len(self.local_supported_versions), self.local_supported_versions)
        self.remote_versions = {}
        if self.rules_force_remote_download or self.rules_version not in self.local_supported_versions:
            self.log.info("Remote rule download needed (force: %s, version available locally: %s)",
                         self.rules_force_remote_download, self.rules_version in self.local_supported_versions)

            self.log.debug("Fetching remote supported versions...")
            self.remote_supported_versions = self.supported_remote_versions()
            self.log.info("Found %d remote supported versions: %s", len(self.remote_supported_versions), self.remote_supported_versions)

            if self.rules_version not in self.remote_supported_versions:
                self.log.error("Version %s not found in remote versions", self.rules_version)
                raise UnsupportedVersion(
                    f"FOCUS version {self.rules_version} not supported. Supported versions: local {self.local_supported_versions} remote {self.remote_supported_versions}"
                )
            else:
                self.log.info("Downloading remote rules for version %s...", self.rules_version)
                download_url = self.remote_versions[self.rules_version]["asset_browser_download_url"]
                self.log.debug("Download URL: %s", download_url)

                if not self.download_remote_version(remote_url=download_url, save_path=self.json_rule_file):
                    self.log.error("Failed to download remote rules file")
                    raise FailedDownloadError(
                        f"Failed to download remote rules file for version {self.rules_version}"
                    )
                else:
                    self.log.info("Remote rules downloaded successfully")
        self.rules = []
        self.column_namespace = column_namespace
        self.json_rules = {}
        self.json_checkfunctions = {}

    def supported_local_versions(self) -> List[str]:
        """Return list of versions from files in rule_set_path."""
        versions = []
        for filename in os.listdir(self.rule_set_path):
            if filename.startswith(self.rules_file_prefix) and filename.endswith(self.rules_file_suffix):
                # extract the part between prefix and suffix
                version = filename[len(self.rules_file_prefix):-len(self.rules_file_suffix)]
                versions.append(version)
        return versions

    def find_release_assets(
        self,
        owner: str = "FinOps-Open-Cost-and-Usage-Spec",
        repo: str = "FOCUS_Spec",
        per_page: int = 100,
        timeout: float = 15.0,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Search GitHub releases for assets whose names start with
        self.rules_files_prefix and end with self.rules_files_suffix.

        Returns a dict of dicts:
        {
            "version": {
                "release_tag": "v1.2",
                "asset_browser_download_url": "<asset_browser_download_url>"
            }
        }
        """
        session = requests.Session()
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            # Optional but helps with routing
            "User-Agent": "focus-validator/asset-scan"
        }

        results: Dict[str, Dict[str, Any]] = {}
        page = 1
        while True:
            url = f"https://api.github.com/repos/{owner}/{repo}/releases"
            params = {"per_page": per_page, "page": page}
            resp = session.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 404:
                raise ValueError(f"Repo not found: {owner}/{repo}")
            if resp.status_code == 401:
                raise PermissionError("Unauthorized (bad or missing token)")
            if resp.status_code == 403:
                # Could be secondary rate limiting or scope issue
                raise RuntimeError(f"Forbidden / rate limited: {resp.text}")
            resp.raise_for_status()

            releases = resp.json()
            if not releases:
                break  # no more pages

            for rel in releases:
                # Filter by draft/prerelease flags
                if (not self.allow_draft_releases and rel.get("draft")):
                    continue
                if (not self.allow_prerelease_releases and rel.get("prerelease")):
                    continue

                assets = rel.get("assets", []) or []
                for asset in assets:
                    name = asset.get("name", "")
                    if name.startswith(self.rules_file_prefix) and name.endswith(self.rules_file_suffix):
                       results[rel.get("tag_name", "").removeprefix("v")] = {
                            "release_tag": rel.get("tag_name"),
                            "asset_browser_download_url": asset.get("browser_download_url"),
                        }
            page += 1

        return results

    def supported_remote_versions(self) -> List[str]:
        """Return list of versions from remote source."""
        # Implement logic to fetch supported remote versions
        self.remote_versions = self.find_release_assets()
        return [v for v in self.remote_versions.keys()]

    def download_remote_version(self, remote_url: str, save_path: str) -> bool:
        """Download the file from remote_url and save it to save_path.
         Returns True if download was successful, False otherwise.
         """
        try:
            response = requests.get(remote_url)
            response.raise_for_status()  # Raise an error for bad status codes
            with open(save_path, 'wb') as file:
                file.write(response.content)
            return True
        except requests.RequestException as e:
            self.log.error("Error downloading file: %s", e)
        return False

    def get_spec_rules_path(self) -> str:
        return self.json_rule_file

    def load(self) -> None:
        self.load_rules()

    def load_rules(self) -> None:
        # Load rules from JSON with dependency resolution
        self.log.info("Loading rules from file: %s", self.json_rule_file)
        self.log.debug("Focus dataset: %s", self.focus_dataset)
        if self.filter_rules:
            self.log.info("Rule filtering active: %s", self.filter_rules)

        self.log.debug("Loading rules with dependency resolution...")
        startTime = time.time()

        self.json_rules, self.json_checkfunctions, rule_order = JsonLoader.load_json_rules_with_dependencies(
            json_rule_file=self.json_rule_file, focus_dataset=self.focus_dataset, filter_rules=self.filter_rules
        )

        loadDuration = time.time() - startTime
        self.log.info("Rules loaded in %.3f seconds", loadDuration)
        self.log.info("Total raw rules: %d", len(self.json_rules))
        self.log.info("Check functions: %d", len(self.json_checkfunctions))
        self.log.info("Rule processing order determined: %d rules", len(rule_order))

        # Process rules in dependency order (topologically sorted)
        processedCount = 0
        dynamicRulesCount = 0
        invalidRulesCount = 0

        self.log.debug("Processing %d rules in dependency order...", len(rule_order))

        for rule_id in rule_order:
            ruleDescription = self.json_rules[rule_id]

            # Check if this is a Dynamic rule - handle specially
            if ruleDescription.get("Type", "").lower() == "dynamic":
                self.log.debug("Processing dynamic rule: %s", rule_id)
                # Create a minimal rule object for Dynamic rules that will be skipped
                dynamic_rule = Rule(
                    check_id=rule_id,
                    column_id=ruleDescription.get("Reference", ""),
                    check="column_required",  # Placeholder check type - will be skipped anyway
                    check_friendly_name=ruleDescription.get("ValidationCriteria", {}).get("MustSatisfy", "Dynamic rule")
                )
                dynamic_rule._rule_type = "Dynamic"
                self.rules.append(dynamic_rule)
                dynamicRulesCount += 1
            else:
                self.log.debug("Processing static rule: %s", rule_id)
                # Use the new method that creates sub-rules for conditions
                rule_objects = Rule.load_json_with_subchecks(ruleDescription, rule_id=rule_id, column_namespace=self.column_namespace)

                for ruleObj in rule_objects:
                    if isinstance(ruleObj, InvalidRule):
                        self.log.warning("Skipping invalid rule: %s", rule_id)
                        invalidRulesCount += 1
                        continue  # Skip invalid rules

                    # Mark rule type for all rules
                    if hasattr(ruleObj, '__dict__'):
                        ruleObj.__dict__['_rule_type'] = ruleDescription.get("Type", "Static")

                    self.rules.append(ruleObj)

            processedCount += 1
            if processedCount % 50 == 0:
                self.log.debug("Processed %d/%d rules", processedCount, len(rule_order))

        self.log.info("Rule processing completed:")
        self.log.info("  Processed: %d rules", processedCount)
        self.log.info("  Valid rules created: %d", len(self.rules))
        self.log.info("  Dynamic rules: %d", dynamicRulesCount)
        self.log.info("  Invalid rules skipped: %d", invalidRulesCount)

    def validate(self, focus_data, connection: Optional[duckdb.DuckDBPyConnection] = None, table_name: Optional[str] = "focus_data") -> ValidationResult:
        self.log.info("Starting rule validation...")
        self.log.debug("Table name: %s", table_name)
        self.log.debug("Using external connection: %s", connection is not None)

        # Generate DuckDB validation checks and checklist
        self.log.debug("Generating DuckDB validation checks...")
        startTime = time.time()

        (
            duckdb_checks,
            checklist,
            compositeRuleIds,
        ) = FocusToDuckDBSchemaConverter.generateDuckDBValidation(
            rules=self.rules
        )

        generationTime = time.time() - startTime
        self.log.info("Generated %d validation checks in %.3f seconds", len(duckdb_checks), generationTime)
        self.log.debug("Checklist contains %d items", len(checklist))

        if connection is None:
            self.log.debug("Creating in-memory DuckDB connection")
            connection = duckdb.connect(":memory:")
            connection.register(table_name, focus_data)
        else:
            self.log.debug("Using provided DuckDB connection")

        # Execute DuckDB validation
        self.log.info("Executing DuckDB validation...")
        executionStartTime = time.time()

        updated_checklist = FocusToDuckDBSchemaConverter.executeDuckDBValidation(
            connection=connection,
            tableName=table_name,
            checks=duckdb_checks,
            checklist=checklist,
            compositeRuleIds=compositeRuleIds
        )

        executionTime = time.time() - executionStartTime
        self.log.info("Validation execution completed in %.3f seconds", executionTime)

        # Process validation results
        self.log.debug("Processing validation results...")
        validation_result = ValidationResult(
            checklist=updated_checklist, failure_cases=None
        )
        validation_result.process_result()

        # Log validation summary
        if updated_checklist:
            passedCount = sum(1 for check in updated_checklist.values()
                            if hasattr(check, 'status') and check.status.name == 'PASS')
            failedCount = len(updated_checklist) - passedCount
            self.log.info("Validation summary: %d passed, %d failed (%.1f%% success rate)",
                         passedCount, failedCount,
                         (passedCount / len(updated_checklist) * 100) if updated_checklist else 0)

        return validation_result
