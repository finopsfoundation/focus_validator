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
    ):
        self.rule_set_path = rule_set_path
        self.rules_file_prefix = rules_file_prefix
        self.rules_version = rules_version
        self.rules_file_suffix = rules_file_suffix
        self.focus_dataset = focus_dataset
        self.filter_rules = filter_rules
        self.applicability_criteria_list = applicability_criteria_list or []
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
        if self.rules_block_remote_download and (
            self.rules_version not in self.local_supported_versions
        ):
            self.log.error(
                "Version %s not found in local versions and remote download blocked",
                self.rules_version,
            )
            raise UnsupportedVersion(
                f"FOCUS version {self.rules_version} not supported. Supported versions: local {self.local_supported_versions}"
            )
        elif (
            self.rules_force_remote_download
            or self.rules_version not in self.local_supported_versions
        ):
            self.log.info(
                "Remote rule download needed (force: %s, version available locally: %s)",
                self.rules_force_remote_download,
                self.rules_version in self.local_supported_versions,
            )

            self.log.debug("Fetching remote supported versions...")
            self.remote_supported_versions = self.supported_remote_versions()
            self.log.info(
                "Found %d remote supported versions: %s",
                len(self.remote_supported_versions),
                self.remote_supported_versions,
            )

            if self.rules_version not in self.remote_supported_versions:
                self.log.error(
                    "Version %s not found in remote versions", self.rules_version
                )
                raise UnsupportedVersion(
                    f"FOCUS version {self.rules_version} not supported. Supported versions: local {self.local_supported_versions} remote {self.remote_supported_versions}"
                )
            else:
                self.log.info(
                    "Downloading remote rules for version %s...", self.rules_version
                )
                download_url = self.remote_versions[self.rules_version][
                    "asset_browser_download_url"
                ]
                self.log.debug("Download URL: %s", download_url)

                if not self.download_remote_version(
                    remote_url=download_url, save_path=self.json_rule_file
                ):
                    self.log.error("Failed to download remote rules file")
                    raise FailedDownloadError(
                        f"Failed to download remote rules file for version {self.rules_version}"
                    )
                else:
                    self.log.info("Remote rules downloaded successfully")
        self.rules = {}
        self.column_namespace = column_namespace
        self.json_rules = {}
        self.json_checkfunctions = {}

    def supported_local_versions(self) -> List[str]:
        """Return list of versions from files in rule_set_path."""
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
                # Could be secondary rate limiting or scope issue
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

                assets = rel.get("assets", []) or []
                for asset in assets:
                    name = asset.get("name", "")
                    if name.startswith(self.rules_file_prefix) and name.endswith(
                        self.rules_file_suffix
                    ):
                        results[rel.get("tag_name", "").removeprefix("v")] = {
                            "release_tag": rel.get("tag_name"),
                            "asset_browser_download_url": asset.get(
                                "browser_download_url"
                            ),
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
        val_plan = JsonLoader.load_json_rules_with_dependencies(
            json_rule_file=self.json_rule_file,
            focus_dataset=self.focus_dataset,
            filter_rules=self.filter_rules,
            applicability_criteria_list=self.applicability_criteria_list,
        )
        self.plan = val_plan
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
                        )

            # 6) Normal finalization (e.g., drop temps, flush logs)
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
            results_by_idx, self._results_by_rule_id(results_by_idx), rules_dict
        )

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
