#!/usr/bin/env python3
import argparse
import json
import logging
from graphviz import Digraph
from focus_validator.rules.spec_rules import ValidationResult
from focus_validator.config_objects.common import ChecklistObjectStatus


def getArgs():
    parser = argparse.ArgumentParser(description='Validation Results Graph Generator.')
    parser.add_argument('-r', '--results-file', type=str, help='Path to validation results JSON file')
    parser.add_argument('--dot-filename', type=str, default='validation_results.dot', help='Output dot filename')
    parser.add_argument('--png-filename', type=str, default='validation_results.png', help='Output png filename')
    parser.add_argument('--logging-level', type=str, default='WARNING', choices={"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}, help='Logging level to use')
    parser.add_argument('--show-passed', action='store_true', help='Include passed checks in visualization')
    parser.add_argument('--show-dependencies-only', action='store_true', help='Only show dependency relationships')
    parser.add_argument('--spec-rules-path', type=str, default=None, help='Path to the FOCUS spec rules JSON (e.g. cr-1.2.json)')
    return parser.parse_args()


class ValidationResultsVisualizer:
    def __init__(self, validationResult=None, resultsFile=None, showPassed=True, showDependenciesOnly=False, spec_rules_path=None):
        self.validationResult = validationResult
        self.resultsFile = resultsFile
        self.logger = logging.getLogger(__name__)
        self.showPassed = showPassed
        self.showDependenciesOnly = showDependenciesOnly
        self.spec_rules_path = spec_rules_path
        self.validationResults = None
        self.dependencyGraph = {}
        self.visualGraph = ValidationGraph(graphName='Validation Results')

    def loadResults(self):
        if self.validationResult:
            # Load directly from ValidationResult object
            checklist = {}
            for checkId, checkObj in self.validationResult.checklist.items():
                checklist[checkId] = {
                    'status': checkObj.status.value if hasattr(checkObj.status, 'value') else str(checkObj.status),
                    'friendly_name': checkObj.friendly_name,
                    'rule_ref': {'check': checkObj.rule_ref.check if hasattr(checkObj.rule_ref, 'check') else 'unknown'},
                    'column_id': checkObj.column_id,
                    'dependencies': getattr(checkObj.rule_ref, 'dependencies', []) if hasattr(checkObj, 'rule_ref') else []
                }

            self.validationResults = {
                'checklist': checklist,
                'failure_cases': self.validationResult.failure_cases.to_dict('records') if self.validationResult.failure_cases is not None else []
            }
        elif self.resultsFile:
            # Load validation results from JSON file
            with open(self.resultsFile, 'r') as f:
                data = json.load(f)

            # Extract checklist and failure cases if present
            checklist = {}
            for checkId, checkData in data.get('checklist', {}).items():
                # Reconstruct ChecklistObject-like structure
                checklist[checkId] = {
                    'status': checkData.get('status', 'unknown'),
                    'friendly_name': checkData.get('friendly_name', checkId),
                    'rule_ref': checkData.get('rule_ref', {}),
                    'column_id': checkData.get('column_id', ''),
                    'dependencies': checkData.get('dependencies', [])
                }

            self.validationResults = {
                'checklist': checklist,
                'failure_cases': data.get('failure_cases', [])
            }
        else:
            raise ValueError("Either validationResult or resultsFile must be provided")

    def buildDependencyGraph(self):
        # Build dependency relationships from original JSON rule structure
        self.dependencyGraph = self._extractDependenciesFromJson()

    def shouldIncludeNode(self, checkId, checkObj):
        # Include all checks by default - filtering only applied when explicitly requested
        status = checkObj.get('status', 'unknown')

        # Only filter out passed checks if showPassed is explicitly set to False
        # Default behavior is now to show everything
        if self.showPassed is False and status == ChecklistObjectStatus.PASSED.value:
            return False

        return True

    def getNodeColor(self, status):
        # Color mapping for different validation statuses
        colorMap = {
            ChecklistObjectStatus.PASSED.value: 'lightgreen',
            ChecklistObjectStatus.FAILED.value: 'lightcoral',
            ChecklistObjectStatus.ERRORED.value: 'orange',
            ChecklistObjectStatus.SKIPPED.value: 'lightgray',
            ChecklistObjectStatus.PENDING.value: 'lightyellow'
        }
        return colorMap.get(status, 'white')

    def getNodeShape(self, checkObj):
        # Different shapes for different types of checks
        checkId = checkObj.get('check_name', '')
        ruleRef = checkObj.get('rule_ref', {})
        checkType = ruleRef.get('check', 'unknown')

        # Special shape for condition rules
        if '_condition' in checkId:
            return 'hexagon'
        elif checkType == 'column_required':
            return 'box'
        elif checkType in ['check_unique', 'value_in', 'sql_query']:
            return 'ellipse'
        elif self._isFormatCheck(checkType):
            return 'diamond'
        else:
            return 'ellipse'

    def _isFormatCheck(self, checkType):
        # Handle both string check types and check objects
        if isinstance(checkType, str):
            return 'format' in checkType.lower()
        
        # Handle FormatCheck objects
        from focus_validator.config_objects.common import FormatCheck
        if isinstance(checkType, FormatCheck):
            return True
            
        # Handle other check objects that might contain format-related checks
        if hasattr(checkType, 'format_type'):
            return True
            
        return False

    def addNodeToGraph(self, checkId, checkObj):
        if not self.shouldIncludeNode(checkId, checkObj):
            return False

        status = checkObj.get('status', 'unknown')
        friendlyName = checkObj.get('friendly_name', checkId)

        # Create node label without status indicator
        label = checkId
        color = self.getNodeColor(status)
        shape = self.getNodeShape(checkObj)

        self.visualGraph.addNode(
            checkId,
            label=label,
            color=color,
            shape=shape,
            customData=self._serializeCheckObj(checkObj)
        )
        return True

    def _serializeCheckObj(self, checkObj):
        # Create a JSON-serializable version of checkObj
        serializable = {}
        
        for key, value in checkObj.items():
            if key == 'rule_ref' and hasattr(value, 'check'):
                # Handle the rule_ref object specially
                ruleRefData = {
                    'check_id': getattr(value, 'check_id', None),
                    'column_id': getattr(value, 'column_id', None),
                    'check_friendly_name': getattr(value, 'check_friendly_name', None)
                }
                
                # Convert the check object to a string representation
                check = getattr(value, 'check', None)
                if check:
                    ruleRefData['check_type'] = self._getCheckTypeString(check)
                
                serializable[key] = ruleRefData
            else:
                # For simple values, include them directly
                try:
                    import json
                    json.dumps(value)  # Test if it's serializable
                    serializable[key] = value
                except (TypeError, ValueError):
                    # If not serializable, convert to string
                    serializable[key] = str(value)
        
        return serializable

    def _getCheckTypeString(self, check):
        # Convert check objects to readable strings
        from focus_validator.config_objects.common import DataTypeCheck, FormatCheck, ValueComparisonCheck
        
        if isinstance(check, str):
            return check
        elif isinstance(check, DataTypeCheck):
            return f"DataType({check.data_type.value})"
        elif isinstance(check, FormatCheck):
            return f"Format({check.format_type})"
        elif isinstance(check, ValueComparisonCheck):
            return f"ValueComparison({check.operator}, {check.value})"
        else:
            return str(type(check).__name__)

    def _extractDependenciesFromJson(self):
        # Extract rule dependencies directly from cr-1.2.json structure
        dependencies = {}
        
        try:
            with open(self.spec_rules_path, 'r') as f:
                rulesData = json.load(f)
            
            conformanceRules = rulesData.get('ConformanceRules', {})
            
            # Extract dependencies for each rule
            for ruleId, ruleData in conformanceRules.items():
                validationCriteria = ruleData.get('ValidationCriteria', {})
                requirement = validationCriteria.get('Requirement', {})
                
                if isinstance(requirement, dict):
                    ruleDependencies = self._extractRuleDependencies(requirement)
                    if ruleDependencies:
                        dependencies[ruleId] = ruleDependencies
            
            return dependencies
            
        except Exception as e:
            self.logger.warning("Could not load dependency file for extraction: %s", str(e))
            self.logger.debug("Dependency extraction failed", exc_info=True)
            return {}
    
    def _extractRuleDependencies(self, requirement):
        # Recursively extract ConformanceRuleId dependencies
        dependencies = []
        
        if isinstance(requirement, dict):
            if requirement.get('CheckFunction') == 'CheckConformanceRule':
                ruleId = requirement.get('ConformanceRuleId')
                if ruleId:
                    dependencies.append(ruleId)
            
            # Handle composite rules (AND/OR with Items)
            items = requirement.get('Items', [])
            for item in items:
                dependencies.extend(self._extractRuleDependencies(item))
        
        return dependencies

    def addDependenciesToGraph(self):
        # Add dependency edges to the graph using original JSON structure
        for parentId, dependencies in self.dependencyGraph.items():
            # Add parent node if it exists in checklist
            if parentId in self.validationResults['checklist']:
                parentObj = self.validationResults['checklist'][parentId]
                parentAdded = self.addNodeToGraph(parentId, parentObj)

                # Add edges from dependencies to parent (dependencies -> parent)
                for depId in dependencies:
                    if depId in self.validationResults['checklist']:
                        depObj = self.validationResults['checklist'][depId]
                        depAdded = self.addNodeToGraph(depId, depObj)

                        # Only add edge if both nodes were added
                        if parentAdded and depAdded:
                            self.visualGraph.addEdge(parentId, depId)

    def addStandaloneNodes(self):
        # Add nodes that don't have dependencies (if not in dependencies-only mode)
        if self.showDependenciesOnly:
            return

        for checkId, checkObj in self.validationResults['checklist'].items():
            # Check if this node is already part of dependency graph
            isInDependencyGraph = (
                checkId in self.dependencyGraph or
                any(checkId in children for children in self.dependencyGraph.values())
            )

            if not isInDependencyGraph:
                self.addNodeToGraph(checkId, checkObj)

    def addParentChildRelationships(self):
        # Add relationships between parent rules and their condition sub-rules
        parent_child_pairs = []

        for checkId, checkObj in self.validationResults['checklist'].items():
            # Check if this is a condition rule (has _condition suffix)
            if '_condition' in checkId:
                # Find the parent rule ID by removing _condition suffix
                parent_id = checkId.replace('_condition', '')
                if parent_id in self.validationResults['checklist']:
                    parent_child_pairs.append((parent_id, checkId))

        # Add parent and child nodes, then connect them with edges
        for parent_id, child_id in parent_child_pairs:
            parent_obj = self.validationResults['checklist'][parent_id]
            child_obj = self.validationResults['checklist'][child_id]

            parent_added = self.addNodeToGraph(parent_id, parent_obj)
            child_added = self.addNodeToGraph(child_id, child_obj)

            # Add edge from parent to child to show the hierarchical relationship
            if parent_added and child_added:
                self.visualGraph.addEdge(parent_id, child_id)

    def generateVisualization(self):
        self.loadResults()
        self.buildDependencyGraph()

        # Add nodes and edges based on configuration
        self.addDependenciesToGraph()
        self.addParentChildRelationships()
        self.addStandaloneNodes()

        self.logger.info(f'Generated graph with {len(self.visualGraph.addedNodes)} nodes and {len(self.visualGraph.addedEdges)} edges')

    def generateDotFile(self, dotFilename):
        self.visualGraph.render(dotFilename, formatType="dot")

    def generatePngFile(self, pngFilename):
        self.visualGraph.render(pngFilename, formatType="png")

    def generateSvgFile(self, svgFilename):
        self.visualGraph.render(svgFilename, formatType="svg")


class ValidationGraph:
    def __init__(self, graphName):
        self.graphName = graphName
        self.dot = None
        self.initDot()
        self.addedNodes = set()
        self.addedEdges = set()
        self.logger = logging.getLogger(__name__)

    def initDot(self):
        # Initialize the DOT graph with better styling for validation results
        self.dot = Digraph(comment=self.graphName)
        self.dot.attr(overlap='false')
        self.dot.attr(rankdir='TB')  # Top to bottom layout
        self.dot.attr('node', style='filled', fontname='Arial')
        self.dot.attr('edge', fontname='Arial')

    def addNode(self, nodeId, customData=None, label=None, shape='ellipse', color='white'):
        if nodeId not in self.addedNodes:
            self.logger.debug(f'Adding node: {nodeId}')
            self.dot.node(
                nodeId,
                label or nodeId,
                shape=shape,
                fillcolor=color,
                customData=json.dumps(customData) if customData else None
            )
            self.addedNodes.add(nodeId)

    def addEdge(self, src, dst, label=None):
        edge = (src, dst)
        if edge not in self.addedEdges:
            self.logger.debug(f'Adding edge: {src} -> {dst}')
            if label:
                self.dot.edge(src, dst, label=label)
            else:
                self.dot.edge(src, dst)
            self.addedEdges.add(edge)

    def render(self, filename, formatType):
        self.dot.render(filename.replace(f'.{formatType}', ''), format=formatType, cleanup=True)


def visualizeValidationResults(validationResult=None, resultsFile=None, dotFilename=None, pngFilename=None, svgFilename=None, showPassed=True, showDependenciesOnly=False, loggingLevel='WARNING', spec_rules_path=None):
    """
    Function to visualize validation results from a ValidationResult object or JSON file.

    Args:
        validationResult: ValidationResult object (takes precedence over resultsFile)
        resultsFile: Path to JSON file containing validation results
        dotFilename: Output filename for DOT format (optional)
        pngFilename: Output filename for PNG format (optional)
        svgFilename: Output filename for SVG format (optional)
        showPassed: Whether to include passed checks in visualization
        showDependenciesOnly: Whether to only show dependency relationships
        loggingLevel: Logging level for output

    Returns:
        ValidationResultsVisualizer instance
    """
    logger = logging.getLogger(__name__)

    visualizer = ValidationResultsVisualizer(
        validationResult=validationResult,
        resultsFile=resultsFile,
        showPassed=showPassed,
        showDependenciesOnly=showDependenciesOnly,
        spec_rules_path=spec_rules_path
    )

    visualizer.generateVisualization()

    if dotFilename:
        visualizer.generateDotFile(dotFilename)

    if pngFilename:
        visualizer.generatePngFile(pngFilename)

    if svgFilename:
        visualizer.generateSvgFile(svgFilename)

    return visualizer


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    args = getArgs()

    if not args.results_file:
        print("Error: --results-file is required when running as script")
        exit(1)

    visualizeValidationResults(
        resultsFile=args.results_file,
        dotFilename=args.dot_filename,
        pngFilename=args.png_filename,
        showPassed=args.show_passed,
        showDependenciesOnly=args.show_dependencies_only,
        loggingLevel=args.logging_level,
        spec_rules_path=args.spec_rules_path
    )