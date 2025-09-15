#!/usr/bin/env python3
import argparse
import json
import logging
from graphviz import Digraph
from focus_validator.rules.spec_rules import ValidationResult
from focus_validator.config_objects.common import ChecklistObjectStatus


def getArgs():
    parser = argparse.ArgumentParser(description='Validation Results Graph Generator.')
    parser.add_argument('-r', '--results-file', type=str, required=True, help='Path to validation results JSON file')
    parser.add_argument('--dot-filename', type=str, default='validation_results.dot', help='Output dot filename')
    parser.add_argument('--png-filename', type=str, default='validation_results.png', help='Output png filename')
    parser.add_argument('--logging-level', type=str, default='WARNING', choices={"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}, help='Logging level to use')
    parser.add_argument('--show-passed', action='store_true', help='Include passed checks in visualization')
    parser.add_argument('--show-dependencies-only', action='store_true', help='Only show dependency relationships')
    return parser.parse_args()


def initLogger(loggingLevel):
    # Simple logger setup
    logging.basicConfig(level=getattr(logging, loggingLevel))
    return logging.getLogger(__name__)


class ValidationResultsVisualizer:
    def __init__(self, resultsFile, logger, showPassed=False, showDependenciesOnly=False):
        self.resultsFile = resultsFile
        self.logger = logger
        self.showPassed = showPassed
        self.showDependenciesOnly = showDependenciesOnly
        self.validationResults = None
        self.dependencyGraph = {}
        self.visualGraph = ValidationGraph(graphName='Validation Results', logger=logger)

    def loadResults(self):
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

    def buildDependencyGraph(self):
        # Build dependency relationships from checklist
        for checkId, checkObj in self.validationResults['checklist'].items():
            dependencies = checkObj.get('dependencies', [])
            for dep in dependencies:
                if dep not in self.dependencyGraph:
                    self.dependencyGraph[dep] = []
                self.dependencyGraph[dep].append(checkId)

    def shouldIncludeNode(self, checkId, checkObj):
        # Filter based on status and user preferences
        status = checkObj.get('status', 'unknown')

        if not self.showPassed and status == ChecklistObjectStatus.PASSED.value:
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
        ruleRef = checkObj.get('rule_ref', {})
        checkType = ruleRef.get('check', 'unknown')

        if checkType == 'column_required':
            return 'box'
        elif checkType in ['check_unique', 'value_in', 'sql_query']:
            return 'ellipse'
        elif 'format' in checkType.lower():
            return 'diamond'
        else:
            return 'ellipse'

    def addNodeToGraph(self, checkId, checkObj):
        if not self.shouldIncludeNode(checkId, checkObj):
            return False

        status = checkObj.get('status', 'unknown')
        friendlyName = checkObj.get('friendly_name', checkId)

        # Create node label with status indicator
        statusSymbol = {
            ChecklistObjectStatus.PASSED.value: '✓',
            ChecklistObjectStatus.FAILED.value: '✗',
            ChecklistObjectStatus.ERRORED.value: '⚠',
            ChecklistObjectStatus.SKIPPED.value: '⊝',
            ChecklistObjectStatus.PENDING.value: '?'
        }.get(status, '?')

        label = f"{statusSymbol} {friendlyName}"
        color = self.getNodeColor(status)
        shape = self.getNodeShape(checkObj)

        self.visualGraph.addNode(
            checkId,
            label=label,
            color=color,
            shape=shape,
            customData=checkObj
        )
        return True

    def addDependenciesToGraph(self):
        # Add dependency edges to the graph
        for parentId, children in self.dependencyGraph.items():
            # Add parent node if it exists in checklist
            if parentId in self.validationResults['checklist']:
                parentObj = self.validationResults['checklist'][parentId]
                parentAdded = self.addNodeToGraph(parentId, parentObj)

                for childId in children:
                    if childId in self.validationResults['checklist']:
                        childObj = self.validationResults['checklist'][childId]
                        childAdded = self.addNodeToGraph(childId, childObj)

                        # Only add edge if both nodes were added
                        if parentAdded and childAdded:
                            self.visualGraph.addEdge(parentId, childId)

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

    def generateVisualization(self):
        self.loadResults()
        self.buildDependencyGraph()

        # Add nodes and edges based on configuration
        self.addDependenciesToGraph()
        self.addStandaloneNodes()

        self.logger.info(f'Generated graph with {len(self.visualGraph.addedNodes)} nodes and {len(self.visualGraph.addedEdges)} edges')

    def generateDotFile(self, dotFilename):
        self.visualGraph.render(dotFilename, formatType="dot")

    def generatePngFile(self, pngFilename):
        self.visualGraph.render(pngFilename, formatType="png")


class ValidationGraph:
    def __init__(self, graphName, logger):
        self.graphName = graphName
        self.dot = None
        self.initDot()
        self.addedNodes = set()
        self.addedEdges = set()
        self.logger = logger

    def initDot(self):
        # Initialize the DOT graph with better styling for validation results
        self.dot = Digraph(comment=self.graphName)
        self.dot.attr(overlap='false')
        self.dot.attr(rankdir='TB')  # Top to bottom layout
        self.dot.attr('node', style='filled', fontname='Arial')
        self.dot.attr('edge', fontname='Arial')

    def addNode(self, nodeId, customData=None, label=None, shape='ellipse', color='white'):
        if nodeId not in self.addedNodes:
            self.logger.info(f'Adding node: {nodeId}')
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
            self.logger.info(f'Adding edge: {src} -> {dst}')
            if label:
                self.dot.edge(src, dst, label=label)
            else:
                self.dot.edge(src, dst)
            self.addedEdges.add(edge)

    def render(self, filename, formatType):
        self.dot.render(filename.replace(f'.{formatType}', ''), format=formatType, cleanup=True)


def visualizeValidationResults(resultsFile, dotFilename=None, pngFilename=None, showPassed=False, showDependenciesOnly=False, loggingLevel='WARNING'):
    """
    Function to visualize validation results from a JSON file.

    Args:
        resultsFile: Path to JSON file containing validation results
        dotFilename: Output filename for DOT format (optional)
        pngFilename: Output filename for PNG format (optional)
        showPassed: Whether to include passed checks in visualization
        showDependenciesOnly: Whether to only show dependency relationships
        loggingLevel: Logging level for output

    Returns:
        ValidationResultsVisualizer instance
    """
    logger = initLogger(loggingLevel)

    visualizer = ValidationResultsVisualizer(
        resultsFile=resultsFile,
        logger=logger,
        showPassed=showPassed,
        showDependenciesOnly=showDependenciesOnly
    )

    visualizer.generateVisualization()

    if dotFilename:
        visualizer.generateDotFile(dotFilename)

    if pngFilename:
        visualizer.generatePngFile(pngFilename)

    return visualizer


if __name__ == '__main__':
    args = getArgs()

    visualizeValidationResults(
        resultsFile=args.results_file,
        dotFilename=args.dot_filename,
        pngFilename=args.png_filename,
        showPassed=args.show_passed,
        showDependenciesOnly=args.show_dependencies_only,
        loggingLevel=args.logging_level
    )