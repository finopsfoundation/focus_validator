import logging
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from focus_validator.config_objects.common import ChecklistObjectStatus

log = logging.getLogger(__name__)


class CompositeCheck(BaseModel):
    # Handles composite rules with AND/OR logic
    logic_operator: Literal["AND", "OR"]
    dependency_rule_ids: List[str]


class InvalidRule(BaseModel):
    rule_path: str
    error: str
    error_type: str


class ValidationCriteria(BaseModel):
    # Required by schema
    must_satisfy: str = Field(..., alias="MustSatisfy")
    keyword: str = Field(..., alias="Keyword")
    requirement: Dict[str, Any] = Field(..., alias="Requirement")
    condition: Dict[str, Any] = Field(..., alias="Condition")
    dependencies: List[str] = Field(..., alias="Dependencies")

    # ---- runtime-only, private storage  ------------
    _precondition: Optional[Dict[str, Any]] = PrivateAttr(default=None)

    @property
    def precondition(self) -> Optional[Dict[str, Any]]:
        """Get the inherited precondition dict (or None if unset)."""
        return self._precondition

    @precondition.setter
    def precondition(self, value: Optional[Dict[str, Any]]) -> None:
        """Set the inherited precondition; must be None or a dict."""
        if value is not None and not isinstance(value, dict):
            raise TypeError("inherited_precondition must be a dict or None")
        if self._precondition is not None:
            raise ValueError(
                "inherited_precondition is already set and cannot be modified"
            )
        self._precondition = value

    # ---- runtime-only, private storage  ------------
    # allow population by field name OR alias
    model_config = ConfigDict(populate_by_name=True)


class ConformanceRule(BaseModel):
    """
    Base rule class that loads spec configs and generates
    a pandera rule that can be validated.
    """

    # Top-level REQUIRED by schema
    function: str = Field(..., alias="Function")
    reference: str = Field(..., alias="Reference")
    entity_type: str = Field(..., alias="EntityType")
    cr_version_introduced: str = Field(..., alias="CRVersionIntroduced")
    status: str = Field(..., alias="Status")
    applicability_criteria: List[str] = Field(..., alias="ApplicabilityCriteria")
    type: str = Field(..., alias="Type")
    validation_criteria: ValidationCriteria = Field(..., alias="ValidationCriteria")
    # ---- runtime-only, private storage  ------------
    _rule_id: str | None = PrivateAttr(default=None)

    def with_rule_id(self, rid: str) -> "ConformanceRule":
        self.rule_id = rid
        return self

    @property
    def rule_id(self) -> Optional[str]:
        return self._rule_id

    @rule_id.setter
    def rule_id(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("rule_id must be a string")
        if self._rule_id is not None:
            raise ValueError("rule_id is already set and cannot be modified")
        self._rule_id = value

    # -----------------------------------------------------------------------------

    def is_active(self) -> bool:
        return self.status == "Active"

    def is_dynamic(self) -> bool:
        return self.type == "Dynamic"

    def is_composite(self) -> bool:
        return self.function == "Composite"

    # Optional metadata
    notes: Optional[str] = Field(None, alias="Notes")

    model_config = ConfigDict(populate_by_name=True)


class ChecklistObject(BaseModel):
    check_name: str
    rule_id: str
    friendly_name: Optional[str] = None
    error: Optional[str] = None
    status: ChecklistObjectStatus
    rule_ref: Union[InvalidRule, ConformanceRule]
    reason: Optional[str] = None
