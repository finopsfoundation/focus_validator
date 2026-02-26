import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import duckdb  # type: ignore[import-untyped]
import requests

from focus_validator.config_objects import JsonLoader, ModelRule
from focus_validator.config_objects.focus_to_duckdb_converter import (
    FocusToDuckDBSchemaConverter,
)
from focus_validator.config_objects.plan_builder import ExecNode, ValidationPlan
from focus_validator.exceptions import (
    FailedDownloadError,
    InvalidRuleException,
    UnsupportedVersion,
)

log = logging.getLogger(__name__)
BuildCheck = Callable[[Any, Dict[int, Dict[str, Any]], Tuple[Any, ...]], Any]
RunCheck = Callable[[Any], Tuple[bool, Dict[str, Any]]]


@dataclass
class ValidationResults:
    """Holds validation outputs in both index-keyed and rule_id-keyed forms."""

    by_idx: Dict[int, Dict[str, Any]]
    by_rule_id: Dict[str, Dict[str, Any]]
    rules: Dict[str, ModelRule]  # rule_id -> full rule object for outputter access
    rules_version: str  # Version of FOCUS rules being validated against
    data_filename: str  # Input data filename for validation context
    data_row_count: int  # Number of rows in the input data
    model_version: str  # Requirements model version from JSON Details section
    focus_dataset: str  # FOCUS dataset name being validated


class SpecRules:
    def __init__(
        self,
        rule_set_path,
        rules_file_prefix,
        rules_version,
        rules_file_suffix,
        focus_dataset,
        filter_rules,
        rules_force_remote_download,
        rules_block_remote_download,
        allow_draft_releases,
        allow_prerelease_releases,
        column_namespace,
        applicability_criteria_list=None,
        transpile_dialect=None,
    ):
        self.rule_set_path = rule_set_path
        self.rules_file_prefix = rules_file_prefix
        self.rules_version = (
            rules_version  # Will be overridden by FOCUSVersion from JSON Details
        )
        self.model_version = (
            "Unknown"  # Will be loaded from ModelVersion in JSON Details
        )
        self.rules_file_suffix = rules_file_suffix
        self.focus_dataset = focus_dataset
        self.filter_rules = filter_rules
        self.applicability_criteria_list = applicability_criteria_list or []
        self.transpile_dialect = transpile_dialect
        self.json_rule_file = os.path.join(
            self.rule_set_path,
            f"{self.rules_file_prefix}{self.rules_version}{self.rules_file_suffix}",
        )
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.rules_force_remote_download = rules_force_remote_download
        self.rules_block_remote_download = rules_block_remote_download
        if self.rules_block_remote_download and self.rules_force_remote_download:
            raise ValueError(
                "rules_block_remote_download and rules_force_remote_download cannot both be True"
            )
        self.allow_draft_releases = allow_draft_releases
        self.allow_prerelease_releases = allow_prerelease_releases
        self.local_supported_versions = self.supported_local_versions()
        self.log.info(
            "Found %d local supported versions: %s",
            len(self.local_supported_versions),
            self.local_supported_versions,
        )
        self.remote_versions = {}

        # Build dict of local versions for semantic matching
        local_versions_dict = {
            v: {"source": "local"} for v in self.local_supported_versions
        }

        # Try semantic version matching for local versions first
        matched_version = self._find_best_version_match(
            self.rules_version, local_versions_dict
        )

        if self.rules_block_remote_download and matched_version is None:
            self.log.error(
                "Version %s not found in local versions and remote download blocked",
                self.rules_version,
            )
            raise UnsupportedVersion(
                f"FOCUS version {self.rules_version} not supported. Supported versions: local {self.local_supported_versions}"
            )
        elif self.rules_force_remote_download or matched_version is None:
            self.log.info(
                "Remote rule download needed (force: %s, version available locally: %s)",
                self.rules_force_remote_download,
                matched_version is not None,
            )

            self.log.debug("Fetching remote supported versions...")
            self.remote_supported_versions = self.supported_remote_versions()
            self.log.info(
                "Found %d remote supported versions: %s",
                len(self.remote_supported_versions),
                self.remote_supported_versions,
            )

            # Try semantic version matching for remote versions
            matched_version = self._find_best_version_match(
                self.rules_version, self.remote_versions
            )

            if matched_version is None:
                self.log.error(
                    "Version %s not found in remote versions", self.rules_version
                )
                raise UnsupportedVersion(
                    f"FOCUS version {self.rules_version} not supported. Supported versions: local {self.local_supported_versions} remote {self.remote_supported_versions}"
                )
            else:
                self.log.info(
                    "Matched requested version %s to %s from remote",
                    self.rules_version,
                    matched_version,
                )
                download_url = self.remote_versions[matched_version][
                    "asset_browser_download_url"
                ]
                filename = self.remote_versions[matched_version]["filename"]
                self.log.debug("Download URL: %s", download_url)

                # Update json_rule_file path to use matched version filename
                self.json_rule_file = os.path.join(self.rule_set_path, filename)

                if not self.download_remote_version(
                    remote_url=download_url, save_path=self.json_rule_file
                ):
                    self.log.error("Failed to download remote rules file")
                    raise FailedDownloadError(
                        f"Failed to download remote rules file for version {self.rules_version}"
                    )
                else:
                    self.log.info("Remote rules downloaded successfully")
        else:
            # Using local version
            self.log.info(
                "Matched requested version %s to %s from local files",
                self.rules_version,
                matched_version,
            )
            # Update json_rule_file path to use matched version
            self.json_rule_file = os.path.join(
                self.rule_set_path,
                f"{self.rules_file_prefix}{matched_version}{self.rules_file_suffix}",
            )
        self.rules = {}
        self.column_namespace = column_namespace
        self.json_rules = {}
        self.json_checkfunctions = {}
        self.plan = None
        self.column_types = {}

    def supported_local_versions(self) -> List[str]:
        """Return list of highest versions from files in rule_set_path.

        Only returns the highest semantic version for each major.minor prefix.
        For example, if both model-1.2.json and model-1.2.0.1.json exist,
        only 1.2.0.1 will be returned.
        """
        versions = []
        for filename in os.listdir(self.rule_set_path):
            if filename.startswith(self.rules_file_prefix) and filename.endswith(
                self.rules_file_suffix
            ):
                # extract the part between prefix and suffix
                version = filename[
                    len(self.rules_file_prefix) : -len(self.rules_file_suffix)
                ]
                versions.append(version)
        return self._filter_to_highest_versions(versions)

    def _parse_version_from_filename(self, filename: str) -> Optional[str]:
        """Extract version from filename like 'model-1.2.0.1.json' -> '1.2.0.1'."""
        if not filename.startswith(self.rules_file_prefix) or not filename.endswith(
            self.rules_file_suffix
        ):
            return None
        version = filename[len(self.rules_file_prefix) : -len(self.rules_file_suffix)]
        return version if version else None

    def _parse_version_tuple(self, version: str) -> Tuple[int, ...]:
        """Convert version string '1.2.0.1' to tuple (1, 2, 0, 1) for comparison."""
        try:
            return tuple(int(x) for x in version.split("."))
        except (ValueError, AttributeError):
            self.log.warning(
                "Malformed version string '%s' - cannot parse as semantic version. "
                "Will sort to bottom. Check for corrupted model filenames.",
                version,
            )
            return (0,)  # Fallback for invalid versions

    def _find_best_version_match(
        self, requested: str, available: Dict[str, Dict[str, Any]]
    ) -> Optional[str]:
        """
        Find the best (highest) semantic version matching the requested prefix.

        Args:
            requested: Version prefix like '1.2' or '1.3'
            available: Dict of available versions with metadata

        Returns:
            Best matching version string, or None if no match found
        """
        # Filter versions that match the requested prefix
        matching = [
            v
            for v in available.keys()
            if v.startswith(requested + ".") or v == requested
        ]

        if not matching:
            return None

        # Sort by semantic version (highest first)
        matching.sort(key=self._parse_version_tuple, reverse=True)
        return matching[0]

    def _filter_to_highest_versions(self, versions: List[str]) -> List[str]:
        """
        Filter version list to only include the highest version for each major.minor prefix.

        For example, given ['1.2', '1.2.0', '1.2.0.1', '1.3', '1.3.0.1']:
        Returns: ['1.2.0.1', '1.3.0.1']

        This ensures the supported versions list only shows versions that would
        actually be used (since semantic matching always picks the highest).

        Args:
            versions: List of version strings

        Returns:
            Filtered list with only highest version per major.minor
        """
        # Group versions by major.minor prefix
        prefix_groups: Dict[str, List[str]] = {}
        for v in versions:
            # Extract major.minor (first two components)
            parts = v.split(".")
            if len(parts) >= 2:
                prefix = f"{parts[0]}.{parts[1]}"
            else:
                prefix = v

            if prefix not in prefix_groups:
                prefix_groups[prefix] = []
            prefix_groups[prefix].append(v)

        # For each prefix, keep only the highest version
        highest_versions = []
        for prefix, group_versions in prefix_groups.items():
            # Sort by semantic version and take the highest
            group_versions.sort(key=self._parse_version_tuple, reverse=True)
            highest_versions.append(group_versions[0])

        # Return sorted by semantic version
        highest_versions.sort(key=self._parse_version_tuple)
        return highest_versions

    def find_release_assets(
        self,
        owner: str = "FinOps-Open-Cost-and-Usage-Spec",
        repo: str = "FOCUS_Spec",
        per_page: int = 100,
        timeout: float = 15.0,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Search GitHub releases for ALL model files across all releases.

        Returns a dict keyed by model VERSION (not release tag):
        {
            "1.2.0.1": {
                "release_tag": "v1.3",
                "filename": "model-1.2.0.1.json",
                "asset_browser_download_url": "<url>"
            },
            "1.3": {
                "release_tag": "v1.3",
                "filename": "model-1.3.json",
                "asset_browser_download_url": "<url>"
            }
        }

        When multiple releases contain the same model version, the first (newest)
        release wins, since GitHub API returns releases in reverse chronological order.
        """
        session = requests.Session()
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "focus-validator/asset-scan",
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
                raise RuntimeError(f"Forbidden / rate limited: {resp.text}")
            resp.raise_for_status()

            releases = resp.json()
            if not releases:
                break  # no more pages

            for rel in releases:
                # Filter by draft/prerelease flags
                if not self.allow_draft_releases and rel.get("draft"):
                    continue
                if not self.allow_prerelease_releases and rel.get("prerelease"):
                    continue

                release_tag = rel.get("tag_name", "")
                assets = rel.get("assets", []) or []

                # Scan ALL model files in this release
                for asset in assets:
                    filename = asset.get("name", "")
                    model_version = self._parse_version_from_filename(filename)

                    if model_version and model_version not in results:
                        # First match wins = newest release, since GitHub API
                        # returns releases newest-first
                        results[model_version] = {
                            "release_tag": release_tag,
                            "filename": filename,
                            "asset_browser_download_url": asset.get(
                                "browser_download_url"
                            ),
                        }

            page += 1

        self.log.debug(
            "Found %d model versions across releases: %s",
            len(results),
            list(results.keys()),
        )
        return results

    def supported_remote_versions(self) -> List[str]:
        """Return list of highest versions from remote source.

        Only returns the highest semantic version for each major.minor prefix.
        For example, if both 1.2 and 1.2.0.1 are available remotely,
        only 1.2.0.1 will be returned since that's what semantic matching would use.
        """
        # Respect block download setting
        if self.rules_block_remote_download:
            self.log.debug(
                "Remote download blocked, returning empty remote versions list"
            )
            return []

        # Implement logic to fetch supported remote versions
        self.remote_versions = self.find_release_assets()
        all_versions = list(self.remote_versions.keys())
        return self._filter_to_highest_versions(all_versions)

    def download_remote_version(self, remote_url: str, save_path: str) -> bool:
        """Download the file from remote_url and save it to save_path.
        Returns True if download was successful, False otherwise.
        """
        try:
            response = requests.get(remote_url)
            response.raise_for_status()  # Raise an error for bad status codes
            with open(save_path, "wb") as file:
                file.write(response.content)
            return True
        except requests.RequestException as e:
            self.log.error("Error downloading file: %s", e)
        return False

    def get_spec_rules_path(self) -> str:
        return self.json_rule_file

    def load(self) -> None:
        self.load_rules()

    def load_rules(self) -> ValidationPlan:
        # Load rules and parse JSON once
        val_plan, column_types, model_data = (
            JsonLoader.load_json_rules_with_dependencies_and_types(
                json_rule_file=self.json_rule_file,
                focus_dataset=self.focus_dataset,
                filter_rules=self.filter_rules,
                applicability_criteria_list=self.applicability_criteria_list,
            )
        )

        # Extract FOCUS version and model version from Details (already parsed above)
        try:
            details = model_data.get("Details", {})
            # Override rules_version with FOCUSVersion from model file
            focus_version = details.get("FOCUSVersion", None)
            if focus_version:
                self.rules_version = focus_version
                self.log.debug("Loaded FOCUS version: %s", self.rules_version)
            else:
                self.log.warning(
                    "FOCUSVersion not found in Details, using requested version: %s",
                    self.rules_version,
                )
            # Load model version
            self.model_version = details.get("ModelVersion", "Unknown")
            self.log.debug("Loaded model version: %s", self.model_version)
        except Exception as e:
            self.log.warning("Failed to load FOCUS/model version: %s", e)
            self.model_version = "Unknown"

        self.plan = val_plan
        self.column_types = column_types
        self._meta = {
            "json_rule_file": self.json_rule_file,
            "focus_dataset": self.focus_dataset,
            "filter_rules": self.filter_rules,
        }
        return val_plan

    def validate(
        self,
        focus_data: Any,
        *,
        connection: Optional[duckdb.DuckDBPyConnection] = None,
        stop_on_first_error: bool = False,
        show_violations: bool = False,
        data_filename: str = "",
        data_row_count: int = 0,
    ) -> ValidationResults:
        """
        Execute the loaded ValidationPlan using DuckDB.
        The converter encapsulates all SQL construction and execution details.

        Args:
          connection: an open duckdb connection
          converter: an instance configured to work with this plan + connection
          stop_on_first_error: abort early when a check fails

        Returns:
          ValidationResults keyed by index and by rule_id.
        """
        if self.plan is None:
            raise RuntimeError("SpecRules.validate() called before load_rules().")

        plan = self.plan
        results_by_idx: Dict[int, Dict[str, Any]] = {}
        converter = FocusToDuckDBSchemaConverter(
            focus_data=focus_data,
            validated_applicability_criteria=self.applicability_criteria_list,
            transpile_dialect=self.transpile_dialect,
            show_violations=show_violations,
            rules_version=self.rules_version,
        )
        # 1) Let the converter prepare schemas, UDFs, temp views, etc.
        if connection is None:
            connection = duckdb.connect(":memory:")
        converter.prepare(conn=connection, plan=plan)

        # Track if we created the connection so we can close it
        connection_created_here = connection is None

        try:
            # 2) Walk layers (easy to parallelize later)
            for layer in plan.layers:
                for idx in layer:
                    node: ExecNode = plan.nodes[idx]
                    setattr(
                        node.rule,
                        "_plan_parents_",
                        {
                            plan.nodes[p].rule_id: results_by_idx[p]
                            for p in node.parent_idxs
                        },
                    )
                    # Collect parents' outputs by index (already executed)
                    parent_results = {
                        pidx: results_by_idx[pidx] for pidx in node.parent_idxs
                    }

                    # 3) Ask converter to build the runnable check for this rule
                    try:
                        check = converter.build_check(
                            rule=node.rule,
                            parent_results_by_idx=parent_results,
                            parent_edges=node.parent_edges,
                            rule_id=node.rule_id,
                            node_idx=idx,
                        )
                    except InvalidRuleException as e:
                        # Make sure the exception mentions this node explicitly
                        raise InvalidRuleException(
                            f"[{node.rule_id} @ idx={idx}] {e}"
                        ) from e

                    # 4) Execute it via converter (runs SQL/relations inside DuckDB)
                    ok, details = converter.run_check(check)

                    # 5) Stash result (index-keyed for speed; include rule_id for convenience)
                    results_by_idx[idx] = {
                        "ok": ok,
                        "details": details,
                        "rule_id": node.rule_id,
                    }

                    # Update converter's global results for dependency propagation
                    converter.update_global_results(idx, ok, details)

                    if stop_on_first_error and not ok:
                        # Allow converter to cleanup if it needs to
                        converter.finalize(success=False, results_by_idx=results_by_idx)
                        rules_dict = {
                            self.plan.nodes[i].rule_id: self.plan.nodes[i].rule
                            for i in results_by_idx.keys()
                        }
                        return ValidationResults(
                            results_by_idx,
                            self._results_by_rule_id(results_by_idx),
                            rules_dict,
                            self.rules_version,
                            data_filename,
                            data_row_count,
                            self.model_version,
                            self.focus_dataset,
                        )

            # 6) POST-PROCESSING: Apply result overrides (non-applicable, composite aggregation, dependency skips)
            # This is the single location where all overrides happen, making logic simple and maintainable
            converter.apply_result_overrides(results_by_idx)

            # 7) Normal finalization (e.g., drop temps, flush logs)
            converter.finalize(success=True, results_by_idx=results_by_idx)

        except Exception:
            # Ensure cleanup on error, then re-raise
            try:
                converter.finalize(success=False, results_by_idx=results_by_idx)
            finally:
                raise
        finally:
            # Close the DuckDB connection if we created it
            if connection_created_here and connection is not None:
                try:
                    connection.close()
                except Exception:
                    # Ignore errors during cleanup
                    pass
        rules_dict = {
            self.plan.nodes[i].rule_id: self.plan.nodes[i].rule
            for i in results_by_idx.keys()
        }
        return ValidationResults(
            results_by_idx,
            self._results_by_rule_id(results_by_idx),
            rules_dict,
            self.rules_version,
            data_filename,
            data_row_count,
            self.model_version,
            self.focus_dataset,
        )

    def explain(self) -> Dict[str, Dict[str, Any]]:
        """
        Generate SQL explanations for all validation rules without executing them.
        This method creates a converter in explain mode and uses the existing
        emit_sql_map functionality to build SQL queries for each rule.

        Returns:
            Dictionary mapping rule_id to explanation dict containing:
            - type: rule type (leaf, composite, reference, skipped)
            - sql: generated SQL query (for leaf rules)
            - check_type: classification of the check
            - generator: generator class used
            - row_condition_sql: condition SQL if applicable
        """
        if self.plan is None:
            raise RuntimeError("SpecRules.explain() called before load_rules().")

        self.log.info(
            "Generating SQL explanations for %d rules...", len(self.plan.nodes)
        )

        # Create converter in explain mode with dummy focus_data
        converter = FocusToDuckDBSchemaConverter(
            focus_data=None,  # No data needed for explain mode
            explain_mode=True,
            validated_applicability_criteria=self.applicability_criteria_list,
            transpile_dialect=self.transpile_dialect,
            show_violations=False,  # Not relevant for explain mode
            rules_version=self.rules_version,
        )

        # Create a minimal connection for explain mode (converter needs it for initialization)
        connection = duckdb.connect(":memory:")

        try:
            # Prepare the converter (this will set up the plan)
            converter.prepare(conn=connection, plan=self.plan)

            # Generate SQL explanations using the existing functionality
            sql_map = converter.emit_sql_map()

            self.log.debug("Generated SQL explanations for %d rules", len(sql_map))
            return sql_map

        except Exception:
            self.log.error("Failed to generate SQL explanations")
            raise
        finally:
            # Clean up the connection
            try:
                connection.close()
            except Exception:
                pass

    # Optional helper(s)
    def _results_by_rule_id(
        self, by_idx: Dict[int, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        if self.plan is None:
            return {}
        return {self.plan.nodes[i].rule_id: res for i, res in by_idx.items()}

    def results_as_markdown(self, results: ValidationResults) -> str:
        lines = ["# Validation Results", ""]
        for rid, res in results.by_rule_id.items():
            # Use ASCII-safe characters for Windows compatibility
            status = ":white_check_mark: PASS" if res.get("ok") else ":x: FAIL"
            lines.append(f"- `{rid}` — {status}")
        return "\n".join(lines)

    def get_column_types(self) -> Dict[str, str]:
        """
        Get the column type mapping extracted from the loaded rules.

        Returns:
            Dict mapping column names to pandas dtype strings
        """
        return self.column_types
