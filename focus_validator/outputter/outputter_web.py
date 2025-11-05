import json
import logging
from typing import Any, Dict


class WebOutputter:
    def __init__(self, output_destination, show_violations=False, focus_dataset=None):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.output_destination = output_destination
        self.result_set = None
        self.show_violations = show_violations
        self.focus_dataset = focus_dataset

    def write(self, results) -> None:
        """
        results: ValidationResults (new type)
        Transform the ValidationResults into the web-friendly format and write to HTML file
        """
        # Transform results to match the SAMPLE_JSCRIPT format
        web_results = self._transform_results(results)

        # Generate HTML content
        html_content = self._generate_html(web_results)

        # Write to file
        with open(self.output_destination, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"Web validation results written to {self.output_destination}")

    def _transform_results(self, results) -> Dict[str, Any]:
        """Transform ValidationResults into web format matching SAMPLE_JSCRIPT structure"""

        # Use the dataset name passed to the constructor
        dataset_name = self.focus_dataset or "Unknown"

        # Calculate summary statistics
        total_columns = 0
        fully_supported = 0
        partially_supported = 0
        not_supported = 0

        # First, identify failed column presence checks
        failed_presence_entities = set()
        for rule_id, entry in results.by_rule_id.items():
            rule_obj = results.rules.get(rule_id)
            if (
                rule_obj
                and rule_obj.function == "Presence"
                and rule_obj.entity_type == "Dataset"
                and not entry.get("ok", False)
            ):
                failed_presence_entities.add(rule_obj.reference)

        # Group rules by column name for easier processing
        columns_data: Dict[str, Dict[str, Any]] = {}

        # First pass: collect all rules by column name
        for rule_id, entry in results.by_rule_id.items():
            # Get the rule object to extract column name from reference field
            rule_obj = results.rules.get(rule_id)
            column_name = self._extract_column_name(rule_obj, dataset_name)

            if column_name not in columns_data:
                columns_data[column_name] = {
                    "name": column_name,
                    "type": "Dimension",  # Default, will be determined later
                    "featureLevel": self._determine_feature_level(rule_id),
                    "requirements": [],
                    "type_rule": None,  # Store Type rule for type determination
                }

            # Store Type rule for later type determination
            if (
                rule_obj
                and hasattr(rule_obj, "function")
                and rule_obj.function == "Type"
            ):
                columns_data[column_name]["type_rule"] = rule_obj

            # Check if this is a column-level rule for a missing column
            if (
                rule_obj
                and rule_obj.entity_type == "Column"
                and rule_obj.reference in failed_presence_entities
            ):
                # Override the entry for column-level rules when column is missing
                modified_entry = entry.copy()
                modified_entry["ok"] = False
                modified_entry["details"] = modified_entry.get("details", {}).copy()
                modified_entry["details"][
                    "message"
                ] = f"Column '{rule_obj.reference}' is missing from the dataset"
                requirement = self._transform_requirement(
                    rule_id, modified_entry, rule_obj
                )
            else:
                # Transform rule entry to requirement format normally
                requirement = self._transform_requirement(rule_id, entry, rule_obj)

            columns_data[column_name]["requirements"].append(requirement)

        # Second pass: determine column types from Type rules
        for column_name, column_data in columns_data.items():
            if column_data["type_rule"]:
                column_data["type"] = self._determine_column_type(
                    column_data["type_rule"]
                )
            # Remove the temporary type_rule field
            del column_data["type_rule"]

        # Sort requirements within each column by rule ID in numeric order
        for column_data in columns_data.values():
            column_data["requirements"].sort(key=self._get_rule_sort_key)

        # Determine status for each column and update counters
        for column_data in columns_data.values():
            status = self._determine_column_status(column_data["requirements"])
            column_data["status"] = status

            total_columns += 1
            if status == "fully":
                fully_supported += 1
            elif status == "partial":
                partially_supported += 1
            else:  # status == "not"
                not_supported += 1

        return {
            "summary": {
                "totalColumns": total_columns,
                "fullySupported": fully_supported,
                "partiallySupported": partially_supported,
                "notSupported": not_supported,
            },
            "columns": list(columns_data.values()),
        }

    def _extract_column_name(self, rule_obj, dataset_name: str = "Unknown") -> str:
        """Extract column name from rule object reference field"""
        if rule_obj and hasattr(rule_obj, "reference"):
            # For dataset-level rules, group them under "Dataset" with dataset name
            if hasattr(rule_obj, "entity_type") and rule_obj.entity_type == "Dataset":
                return f"Dataset ({dataset_name})"
            # For column-level rules, use the reference (entity name)
            return rule_obj.reference
        return "Unknown"

    def _determine_column_type(self, rule_obj) -> str:
        """Determine if column is Metric or Dimension based on Type validation function"""
        # Look for Type function rules to determine the data type
        if rule_obj and hasattr(rule_obj, "function") and rule_obj.function == "Type":
            if hasattr(rule_obj, "validation_criteria") and hasattr(
                rule_obj.validation_criteria, "requirement"
            ):
                requirement = rule_obj.validation_criteria.requirement
                if isinstance(requirement, dict) and "CheckFunction" in requirement:
                    check_function = requirement["CheckFunction"]
                    # Numeric types are Metrics, all others are Dimensions
                    if check_function == "TypeDecimal":
                        return "Metric"
                    else:  # TypeString, TypeDateTime, etc.
                        return "Dimension"

        # Default to Dimension for non-Type rules or special cases like Dataset
        return "Dimension"

    def _determine_feature_level(self, rule_id: str) -> str:
        """Determine feature level from rule ID"""
        # Extract the last part of rule_id (M=Mandatory, C=Conditional, O=Optional)
        if rule_id.endswith("-M"):
            return "Mandatory"
        elif rule_id.endswith("-C"):
            return "Conditional"
        elif rule_id.endswith("-O"):
            return "Optional"
        return "Unknown"

    def _transform_requirement(
        self, rule_id: str, entry: Dict[str, Any], rule_obj
    ) -> Dict[str, Any]:
        """Transform a single rule entry into a requirement object"""
        details = entry.get("details", {})
        message = details.get("message", "")

        # Get rule type and must_satisfy directly from ValidationCriteria
        criteria = rule_obj.validation_criteria
        rule_type = criteria.keyword
        must_satisfy = criteria.must_satisfy

        return {
            "rule": rule_type,
            "ruleId": rule_id,
            "text": must_satisfy,
            "passed": entry.get("ok", False),
            "errorMessage": (
                "" if entry.get("ok", False) else (message or f"Rule {rule_id} failed")
            ),
            "entity": self._get_rule_entity(rule_obj),
            "function": self._get_rule_function(rule_obj),
            "entityType": self._get_rule_entity_type(rule_obj),
            "ruleType": self._get_rule_type(rule_obj),
        }

    def _determine_column_status(self, requirements: list) -> str:
        """Determine overall column status based on requirements"""
        if not requirements:
            return "not"

        passed_count = sum(1 for req in requirements if req["passed"])
        total_count = len(requirements)

        if passed_count == 0:
            return "not"
        elif passed_count == total_count:
            return "fully"
        else:
            return "partial"

    def _get_rule_entity(self, rule_obj) -> str:
        """Extract entity name from rule object (Reference field)"""
        if rule_obj and hasattr(rule_obj, "reference"):
            return rule_obj.reference
        return "Unknown"

    def _get_rule_function(self, rule_obj) -> str:
        """Extract function name from rule object"""
        if rule_obj and hasattr(rule_obj, "function"):
            return rule_obj.function
        return "Unknown"

    def _get_rule_entity_type(self, rule_obj) -> str:
        """Extract entity type from rule object (Column, Dataset, Attribute)"""
        if rule_obj and hasattr(rule_obj, "entity_type"):
            return rule_obj.entity_type
        return "Unknown"

    def _get_rule_type(self, rule_obj) -> str:
        """Extract rule type from rule object (Static, Dynamic)"""
        if rule_obj and hasattr(rule_obj, "type"):
            return rule_obj.type
        return "Unknown"

    def _get_rule_sort_key(self, requirement: Dict[str, Any]) -> tuple:
        """Generate sort key for requirement based on rule ID to ensure numeric ordering"""
        rule_id = requirement.get("ruleId", "")

        try:
            # Parse rule ID format: EntityName-C-###-Level or EntityName-D-###-Level
            parts = rule_id.split("-")
            if len(parts) >= 4:
                entity = parts[0]
                rule_type = parts[1]  # C for Column, D for Dataset, etc.
                number_str = parts[2]
                level = parts[3]

                # Convert number to integer for proper numeric sorting
                number = int(number_str) if number_str.isdigit() else 999

                # Return tuple for sorting: (entity, rule_type, number, level)
                # This ensures C-000 comes before C-001, C-001 before C-002, etc.
                return (entity, rule_type, number, level)
            else:
                # Fallback for unexpected rule ID formats
                return (rule_id, 0, 999, "Z")
        except (ValueError, IndexError):
            # Handle any parsing errors by putting problematic rules at the end
            return (rule_id, 0, 999, "Z")

    def _generate_html(self, web_results: Dict[str, Any]) -> str:
        """Generate complete HTML page with embedded JavaScript data"""

        # Convert results to JSON for embedding in JavaScript
        results_json = json.dumps(web_results, indent=2)

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FOCUS Validation Results</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: rgb(113, 35, 152);
            color: white;
            padding: 50px 20px;
            display: flex;
            align-items: center;
            position: relative;
        }}
        .header-logo {{
            height: 200px;
            width: auto;
            position: absolute;
            left: 20px;
        }}
        .header-content {{
            flex: 1;
            text-align: center;
        }}
        .header-content h1 {{
            margin: 0;
            font-size: 2rem;
        }}
        .header-content p {{
            margin: 5px 0 0 0;
            opacity: 0.9;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 20px;
            background: #f8fafc;
        }}
        .summary-card {{
            background: white;
            padding: 15px;
            border-radius: 6px;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: all 0.2s ease;
        }}
        .summary-card:hover {{
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            transform: translateY(-2px);
        }}
        .summary-card.active {{
            background: rgb(113, 35, 152);
            color: white;
        }}
        .summary-card.active .summary-number {{
            color: white !important;
        }}
        .summary-card.active .summary-label {{
            color: white !important;
        }}
        .summary-number {{
            font-size: 2rem;
            font-weight: bold;
            color: #1e293b;
        }}
        .summary-label {{
            color: #64748b;
            font-size: 0.875rem;
        }}
        .columns-grid {{
            display: grid;
            gap: 16px;
            padding: 20px;
        }}
        .column-card {{
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            overflow: hidden;
        }}
        .column-header {{
            padding: 15px;
            background: #f1f5f9;
            border-bottom: 1px solid #e2e8f0;
        }}
        .column-name {{
            font-size: 1.125rem;
            font-weight: 600;
            color: #1e293b;
        }}
        .column-meta {{
            display: flex;
            gap: 15px;
            margin-top: 5px;
            font-size: 0.875rem;
            color: #64748b;
        }}
        .status-badge {{
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .status-fully {{ background: #dcfce7; color: #16a34a; }}
        .status-partial {{ background: #fef3c7; color: #d97706; }}
        .status-not {{ background: #fee2e2; color: #dc2626; }}
        .requirements {{
            padding: 15px;
        }}
        .requirement {{
            display: flex;
            align-items: flex-start;
            gap: 10px;
            padding: 8px 0;
            border-bottom: 1px solid #f1f5f9;
        }}
        .requirement:last-child {{
            border-bottom: none;
        }}
        .requirement-icon {{
            font-size: 1.125rem;
            margin-top: 2px;
        }}
        .requirement-icon.passed {{ color: #16a34a; }}
        .requirement-icon.failed {{ color: #dc2626; }}
        .requirement-content {{
            flex: 1;
        }}
        .requirement-rule {{
            font-weight: 600;
            font-size: 0.75rem;
            color: #6366f1;
            margin-bottom: 2px;
        }}
        .requirement-text {{
            color: #374151;
            font-size: 0.875rem;
            line-height: 1.4;
        }}
        .requirement-error {{
            color: #dc2626;
            font-size: 0.75rem;
            margin-top: 4px;
            font-style: italic;
        }}
        .requirement-meta {{
            margin-top: 4px;
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }}
        .meta-tag {{
            background: #f1f5f9;
            color: #475569;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.7rem;
            font-weight: 500;
        }}
        .filter-controls {{
            padding: 20px;
            border-bottom: 1px solid #e2e8f0;
            background: #f8fafc;
        }}
        .filter-group {{
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }}
        .filter-label {{
            font-weight: 500;
            color: #374151;
        }}
        select, input {{
            padding: 6px 12px;
            border: 1px solid #d1d5db;
            border-radius: 4px;
            font-size: 0.875rem;
        }}
        .checkbox-group {{
            display: flex;
            gap: 15px;
            align-items: center;
        }}
        .checkbox-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .checkbox-item input[type="checkbox"] {{
            margin: 0;
            padding: 0;
            width: auto;
            height: auto;
        }}
        .checkbox-item label {{
            margin: 0;
            font-weight: normal;
            cursor: pointer;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <img src="data:image/png;base64,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" alt="FOCUS Logo" class="header-logo" id="focus-logo">
            <div class="header-content">
                <h1>Validation Results</h1>
                <p>Report generated on {self._get_current_datetime()}</p>
            </div>
        </div>

        <div id="summary" class="summary">
            <!-- Summary will be populated by JavaScript -->
        </div>

        <div class="filter-controls">
            <div class="filter-group">
                <label class="filter-label">Filter by status:</label>
                <select id="statusFilter">
                    <option value="all">All</option>
                    <option value="fully">Fully Supported</option>
                    <option value="partial">Partially Supported</option>
                    <option value="not">Not Supported</option>
                </select>

                <label class="filter-label">Search columns:</label>
                <input type="text" id="searchInput" placeholder="Column name...">

                <label class="filter-label">Filter by rule type:</label>
                <div class="checkbox-group">
                    <div class="checkbox-item">
                        <input type="checkbox" id="mustFilter" checked>
                        <label for="mustFilter">MUST</label>
                    </div>
                    <div class="checkbox-item">
                        <input type="checkbox" id="shouldFilter" checked>
                        <label for="shouldFilter">SHOULD</label>
                    </div>
                    <div class="checkbox-item">
                        <input type="checkbox" id="mayFilter">
                        <label for="mayFilter">MAY</label>
                    </div>
                </div>
            </div>
        </div>

        <div id="columnsContainer" class="columns-grid">
            <!-- Columns will be populated by JavaScript -->
        </div>
    </div>

    <script>
        // Embed the validation results
        const results = {results_json};

        // Render the page
        document.addEventListener('DOMContentLoaded', function() {{
            renderSummary(results.summary);
            renderColumns(results.columns);
            setupFiltering(results.columns);
            // Initialize with "all" filter active
            updateSummaryCardActive('all');

            // Handle logo loading
            const logo = document.getElementById('focus-logo');
            logo.onerror = function() {{
                // Hide the logo if it fails to load (placeholder base64)
                this.style.display = 'none';
                // Keep the left alignment even without logo
                const header = document.querySelector('.header');
                header.style.justifyContent = 'flex-start';
            }};
        }});

        function renderSummary(summary) {{
            const summaryContainer = document.getElementById('summary');
            summaryContainer.innerHTML = `
                <div class="summary-card" data-filter="all" onclick="filterByStatus('all')">
                    <div class="summary-number">${{summary.totalColumns}}</div>
                    <div class="summary-label">Total Columns</div>
                </div>
                <div class="summary-card" data-filter="fully" onclick="filterByStatus('fully')">
                    <div class="summary-number" style="color: #16a34a">${{summary.fullySupported}}</div>
                    <div class="summary-label">Fully Supported</div>
                </div>
                <div class="summary-card" data-filter="partial" onclick="filterByStatus('partial')">
                    <div class="summary-number" style="color: #d97706">${{summary.partiallySupported}}</div>
                    <div class="summary-label">Partially Supported</div>
                </div>
                <div class="summary-card" data-filter="not" onclick="filterByStatus('not')">
                    <div class="summary-number" style="color: #dc2626">${{summary.notSupported}}</div>
                    <div class="summary-label">Not Supported</div>
                </div>
            `;
        }}

        function renderColumns(columns) {{
            const container = document.getElementById('columnsContainer');
            container.innerHTML = columns.map(column => `
                <div class="column-card" data-status="${{column.status}}" data-name="${{column.name.toLowerCase()}}">
                    <div class="column-header">
                        <div class="column-name">${{column.name}}</div>
                        <div class="column-meta">
                            <span>Type: ${{column.type}}</span>
                            <span>Level: ${{column.featureLevel}}</span>
                            <span class="status-badge status-${{column.status}}">
                                ${{column.status === 'fully' ? 'Fully Supported' :
                                  column.status === 'partial' ? 'Partially Supported' : 'Not Supported'}}
                            </span>
                        </div>
                    </div>
                    <div class="requirements">
                        ${{column.requirements.map(req => `
                            <div class="requirement" data-rule-type="${{req.rule}}">
                                <div class="requirement-icon ${{req.passed ? 'passed' : 'failed'}}">
                                    ${{req.passed ? '✅' : '❌'}}
                                </div>
                                <div class="requirement-content">
                                    <div class="requirement-rule">${{req.rule}} ${{req.ruleId}}</div>
                                    <div class="requirement-text">${{req.text}}</div>
                                    ${{req.entity && req.function && req.entityType ? `
                                        <div class="requirement-meta">
                                            <span class="meta-tag">Entity: ${{req.entity}}</span>
                                            <span class="meta-tag">Function: ${{req.function}}</span>
                                            <span class="meta-tag">Type: ${{req.entityType}}</span>
                                            <span class="meta-tag">Rule: ${{req.ruleType}}</span>
                                        </div>
                                    ` : ''}}
                                    ${{!req.passed && req.errorMessage ? `<div class="requirement-error">${{req.errorMessage}}</div>` : ''}}
                                </div>
                            </div>
                        `).join('')}}
                    </div>
                </div>
            `).join('');
        }}

        function setupFiltering(columns) {{
            const statusFilter = document.getElementById('statusFilter');
            const searchInput = document.getElementById('searchInput');
            const mustFilter = document.getElementById('mustFilter');
            const shouldFilter = document.getElementById('shouldFilter');
            const mayFilter = document.getElementById('mayFilter');

            function getRuleTypeCategory(ruleType) {{
                // MUST includes: MUST, MUST NOT
                if (ruleType === 'MUST' || ruleType === 'MUST NOT') return 'must';
                // SHOULD includes: SHOULD, SHOULD NOT, RECOMMENDED, NOT RECOMMENDED
                if (ruleType === 'SHOULD' || ruleType === 'SHOULD NOT' ||
                    ruleType === 'RECOMMENDED' || ruleType === 'NOT RECOMMENDED') return 'should';
                // MAY includes: MAY, OPTIONAL
                if (ruleType === 'MAY' || ruleType === 'OPTIONAL') return 'may';
                return 'other';
            }}

            function filterColumns() {{
                const statusValue = statusFilter.value;
                const searchValue = searchInput.value.toLowerCase();
                const showMust = mustFilter.checked;
                const showShould = shouldFilter.checked;
                const showMay = mayFilter.checked;

                let totalColumns = 0;
                let fullySupported = 0;
                let partiallySupported = 0;
                let notSupported = 0;

                document.querySelectorAll('.column-card').forEach(card => {{
                    const name = card.dataset.name;
                    const nameMatch = searchValue === '' || name.includes(searchValue);

                    // Check requirements and calculate new status based on visible rules
                    const requirements = card.querySelectorAll('.requirement');
                    let visibleRequirements = [];
                    let ruleTypeMatch = false;

                    requirements.forEach(req => {{
                        const ruleType = req.dataset.ruleType;
                        const category = getRuleTypeCategory(ruleType);

                        if ((category === 'must' && showMust) ||
                            (category === 'should' && showShould) ||
                            (category === 'may' && showMay)) {{
                            ruleTypeMatch = true;
                            req.style.display = 'flex';
                            // Determine if this requirement passed based on its icon
                            const icon = req.querySelector('.requirement-icon');
                            const passed = icon && icon.classList.contains('passed');
                            visibleRequirements.push(passed);
                        }} else {{
                            req.style.display = 'none';
                        }}
                    }});

                    // Calculate new status based on visible requirements only
                    let newStatus = 'not';
                    if (visibleRequirements.length > 0) {{
                        const passedCount = visibleRequirements.filter(passed => passed).length;
                        if (passedCount === visibleRequirements.length) {{
                            newStatus = 'fully';
                        }} else if (passedCount > 0) {{
                            newStatus = 'partial';
                        }} else {{
                            newStatus = 'not';
                        }}
                    }}

                    // Update the card's visual status
                    const statusBadge = card.querySelector('.status-badge');
                    if (statusBadge) {{
                        statusBadge.className = `status-badge status-${{newStatus}}`;
                        statusBadge.textContent = newStatus === 'fully' ? 'Fully Supported' :
                                                 newStatus === 'partial' ? 'Partially Supported' : 'Not Supported';
                    }}

                    // Check if card should be shown based on status filter
                    const statusMatch = statusValue === 'all' || newStatus === statusValue;
                    const shouldShowCard = statusMatch && nameMatch && ruleTypeMatch;
                    card.style.display = shouldShowCard ? 'block' : 'none';

                    // Count for summary if card matches name filter and has visible requirements
                    if (nameMatch && ruleTypeMatch) {{
                        totalColumns++;
                        if (newStatus === 'fully') fullySupported++;
                        else if (newStatus === 'partial') partiallySupported++;
                        else notSupported++;
                    }}
                }});

                // Update summary cards with new counts
                updateSummaryCounts({{
                    totalColumns: totalColumns,
                    fullySupported: fullySupported,
                    partiallySupported: partiallySupported,
                    notSupported: notSupported
                }});

                // Update summary card active state
                updateSummaryCardActive(statusValue);
            }}

            statusFilter.addEventListener('change', filterColumns);
            searchInput.addEventListener('input', filterColumns);
            mustFilter.addEventListener('change', filterColumns);
            shouldFilter.addEventListener('change', filterColumns);
            mayFilter.addEventListener('change', filterColumns);
        }}

        function updateSummaryCounts(counts) {{
            // Update the summary card numbers and colors
            const summaryCards = document.querySelectorAll('.summary-card');
            summaryCards.forEach(card => {{
                const filter = card.dataset.filter;
                const numberEl = card.querySelector('.summary-number');

                if (filter === 'all') {{
                    numberEl.textContent = counts.totalColumns;
                }} else if (filter === 'fully') {{
                    numberEl.textContent = counts.fullySupported;
                    numberEl.style.color = counts.fullySupported > 0 ? '#16a34a' : '#64748b';
                }} else if (filter === 'partial') {{
                    numberEl.textContent = counts.partiallySupported;
                    numberEl.style.color = counts.partiallySupported > 0 ? '#d97706' : '#64748b';
                }} else if (filter === 'not') {{
                    numberEl.textContent = counts.notSupported;
                    numberEl.style.color = counts.notSupported > 0 ? '#dc2626' : '#64748b';
                }}
            }});
        }}

        function filterByStatus(status) {{
            const statusFilter = document.getElementById('statusFilter');
            statusFilter.value = status;

            // Trigger the existing filter function
            const event = new Event('change');
            statusFilter.dispatchEvent(event);
        }}

        function updateSummaryCardActive(activeStatus) {{
            document.querySelectorAll('.summary-card').forEach(card => {{
                if (card.dataset.filter === activeStatus) {{
                    card.classList.add('active');
                }} else {{
                    card.classList.remove('active');
                }}
            }});
        }}
    </script>
</body>
</html>"""

        return html_template

    def _get_current_datetime(self) -> str:
        """Get current date and time for the report"""
        from datetime import datetime

        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


SAMPLE_JSCRIPT = """
const Results = {
            summary: {
                totalColumns: 43,
                fullySupported: 38,
                partiallySupported: 3,
                notSupported: 2
            },
            columns: [
                {
                    name: 'BilledCost',
                    type: 'Metric',
                    featureLevel: 'Mandatory',
                    status: 'fully',
                    requirements: [
                        { rule: 'MUST', text: 'Must be present in dataset', passed: true, errorMessage: '' },
                        { rule: 'MUST', text: 'Must be of type Decimal', passed: true, errorMessage: '' },
                        { rule: 'MUST NOT', text: 'Must not be null', passed: true, errorMessage: '' },
                        { rule: 'MUST', text: 'Must conform to NumericFormat', passed: true, errorMessage: '' }
                    ]
                },
                {
                    name: 'BillingAccountId',
                    type: 'Dimension',
                    featureLevel: 'Mandatory',
                    status: 'fully',
                    requirements: [
                        { rule: 'MUST', text: 'Must be present in dataset', passed: true, errorMessage: '' },
                        { rule: 'MUST', text: 'Must be of type String', passed: true, errorMessage: '' },
                        { rule: 'MUST', text: 'Must conform to StringHandling', passed: true, errorMessage: '' }
                    ]
                },
                {
                    name: 'CommitmentDiscountId',
                    type: 'Dimension',
                    featureLevel: 'Conditional',
                    status: 'partial',
                    requirements: [
                        { rule: 'MUST', text: 'Must be present when commitment discounts exist', passed: true, errorMessage: '' },
                        { rule: 'MUST', text: 'Must be of type String', passed: true, errorMessage: '' },
                        { rule: 'SHOULD', text: 'Should include provider-specific identifier', passed: false, errorMessage: 'Column contains generic identifiers instead of provider-specific format' }
                    ]
                },
                {
                    name: 'InvoiceIssuerName',
                    type: 'Dimension',
                    featureLevel: 'Mandatory',
                    status: 'not',
                    requirements: [
                        { rule: 'MUST', text: 'Must be present in dataset', passed: false, errorMessage: 'Column is missing from dataset' },
                        { rule: 'MUST', text: 'Must be of type String', passed: false, errorMessage: 'Cannot validate type - column not present' }
                    ]
                },
                {
                    name: 'PricingUnit',
                    type: 'Dimension',
                    featureLevel: 'Conditional',
                    status: 'partial',
                    requirements: [
                        { rule: 'MUST', text: 'Must be present when usage-based charges exist', passed: true, errorMessage: '' },
                        { rule: 'SHOULD', text: 'Should follow standard unit naming', passed: false, errorMessage: 'Found non-standard units: "compute-hours" should be "Hours"' },
                        { rule: 'MAY', text: 'May include provider-specific units', passed: true, errorMessage: '' }
                    ]
                }
            ]
        };
"""
