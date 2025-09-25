import os
import requests
from typing import List, Dict, Any, Optional


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


def convert_missing_column_errors(df, checklist):
    def process_row(row):
        if (
            row["schema_context"] == "DataFrameSchema"
            and row["check"] == "column_in_dataframe"
        ):
            for check_name, check_obj in checklist.items():
                if (
                    row["failure_case"] == check_obj.column_id
                    and check_obj.rule_ref.check == "column_required"
                ):
                    row["check"] = f"{check_name}:::{check_obj.friendly_name}"
                    row["column"] = check_obj.column_id
                    row["failure_case"] = None
                    return row
        else:
            return row

    filtered_df = df.apply(process_row, axis=1)
    return filtered_df


def convert_dtype_column_errors(df, checklist):
    def process_row(row):
        if row["schema_context"] == "Column" and row["check"].startswith("dtype"):
            for check_name, check_obj in checklist.items():
                if row["column"] == check_obj.column_id:
                    row["check"] = f"{check_name}:::{check_obj.friendly_name}"
                    row["column"] = check_obj.column_id
                    row["failure_case"] = None
                    return row
        else:
            return row

    filtered_df = df.apply(process_row, axis=1)
    return filtered_df


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
        self, rule_set_path, rules_file_prefix, rules_version, rules_file_suffix, rule_prefix, rules_force_remote_download, allow_draft_releases, allow_prerelease_releases, column_namespace,
    ):
        self.rule_set_path = rule_set_path
        self.rules_file_prefix = rules_file_prefix
        self.rules_version = rules_version
        self.rules_file_suffix = rules_file_suffix
        self.json_rule_file = os.path.join(
            self.rule_set_path, f"{self.rules_file_prefix}{self.rules_version}{self.rules_file_suffix}"
        )
        self.rules_force_remote_download = rules_force_remote_download
        self.allow_draft_releases = allow_draft_releases
        self.allow_prerelease_releases = allow_prerelease_releases
        self.local_supported_versions = self.supported_local_versions()
        self.remote_versions = {}
        if self.rules_force_remote_download or self.rules_version not in self.local_supported_versions:
            self.remote_supported_versions = self.supported_remote_versions()
            if self.rules_version not in self.remote_supported_versions:
                raise UnsupportedVersion(
                    f"FOCUS version {self.rules_version} not supported. Supported versions: local {self.local_supported_versions} remote {self.remote_supported_versions}"
                )
            else:
                if not self.download_remote_version(
                    remote_url=self.remote_versions[self.rules_version]["asset_browser_download_url"],
                    save_path=self.json_rule_file
                ):
                    raise FailedDownloadError(
                        f"Failed to download remote rules file for version {self.rules_version}"
                    )
        self.rule_prefix = rule_prefix
        self.rules = []
        self.column_namespace = column_namespace
        self.json_rules = {}
        self.json_checkfunctions = {}

    def supported_local_versions(self):
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

    def supported_remote_versions(self):
        """Return list of versions from remote source."""
        # Implement logic to fetch supported remote versions
        self.remote_versions = self.find_release_assets()
        return [v for v in self.remote_versions.keys()]

    def download_remote_version(self, remote_url: str, save_path: str):
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
            print(f"Error downloading file: {e}")
        return False

    def load(self):
        self.load_rules()

    def load_rules(self):
        # Load rules from JSON with dependency resolution
        json_rules_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'rules', f'cr-{self.rules_version}.json')
        self.json_rules, self.json_checkfunctions, rule_order = JsonLoader.load_json_rules_with_dependencies(
            json_rule_file=self.json_rule_file, rule_prefix=self.rule_prefix
        )

        # Process rules in dependency order (topologically sorted)
        for rule_id in rule_order:
            ruleDescription = self.json_rules[rule_id]
            
            # Process both static and dynamic rules within version compatibility
            if float(ruleDescription["CRVersionIntroduced"]) <= float(self.rules_version):
                # Check if this is a Dynamic rule - handle specially
                if ruleDescription.get("Type", "").lower() == "dynamic":
                    # Create a minimal rule object for Dynamic rules that will be skipped
                    dynamic_rule = Rule(
                        check_id=rule_id,
                        column_id=ruleDescription.get("Reference", ""),
                        check="column_required",  # Placeholder check type - will be skipped anyway
                        check_friendly_name=ruleDescription.get("ValidationCriteria", {}).get("MustSatisfy", "Dynamic rule")
                    )
                    dynamic_rule._rule_type = "Dynamic"
                    self.rules.append(dynamic_rule)
                else:
                    # Use the new method that creates sub-rules for conditions
                    rule_objects = Rule.load_json_with_subchecks(ruleDescription, rule_id=rule_id, column_namespace=self.column_namespace)

                    for ruleObj in rule_objects:
                        if isinstance(ruleObj, InvalidRule):
                            continue  # Skip invalid rules

                        # Mark rule type for all rules
                        if hasattr(ruleObj, '__dict__'):
                            ruleObj.__dict__['_rule_type'] = ruleDescription.get("Type", "Static")

                        self.rules.append(ruleObj)

    def validate(self, focus_data, connection: Optional[duckdb.DuckDBPyConnection] = None):
        # Generate DuckDB validation checks and checklist
        (
            duckdb_checks,
            checklist,
        ) = FocusToDuckDBSchemaConverter.generateDuckDBValidation(
            rules=self.rules
        )

        if connection is None:
            connection = duckdb.connect(":memory:")
            connection.register("focus_data", focus_data)
            tableName = "focus_data"
        else:
            tableName = "focus_data"  # Default table name, could be parameterized

        # Execute DuckDB validation
        updated_checklist = FocusToDuckDBSchemaConverter.executeDuckDBValidation(
            connection=connection,
            tableName=tableName,
            checks=duckdb_checks,
            checklist=checklist
        )

        # No failure cases for now
        validation_result = ValidationResult(
            checklist=updated_checklist, failure_cases=None
        )
        validation_result.process_result()
        return validation_result
