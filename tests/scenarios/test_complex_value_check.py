import unittest
import pandas as pd
from io import StringIO
from helper import load_rule_data_from_file
from helper import SpecRulesFromData

class TestComplexValueCheck(unittest.TestCase):
    """Test complex value check rule."""
    
    def setUp(self):
        self.rule_data = load_rule_data_from_file("base_rule_data.json")
        self.rule_data['ModelDatasets'] = {
            "CostAndUsage": {
                "ModelRules": ["ServiceSubcategory-C-004-M"]
            }
        }
        self.rule_data["ModelRules"] = {
            "ServiceSubcategory-C-004-M": {
                "Function": "Validation",
                "Reference": "ServiceSubcategory",
                "EntityType": "Column",
                "Notes": "",
                "ModelVersionIntroduced": "1.2",
                "Status": "Active",
                "ApplicabilityCriteria": [],
                "Type": "Static",
                "ValidationCriteria": {
                "MustSatisfy": "ServiceSubcategory MUST have one and only one parent ServiceCategory as specified in the allowed values below.",
                "Keyword": "MUST",
                "Requirement": {
                    "CheckFunction": "OR",
                    "Items": [
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "AI Platforms"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "AI and Machine Learning"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Bots"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "AI and Machine Learning"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Generative AI"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "AI and Machine Learning"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Machine Learning"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "AI and Machine Learning"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Natural Language Processing"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "AI and Machine Learning"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (AI and Machine Learning)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "AI and Machine Learning"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Analytics Platforms"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Analytics"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Business Intelligence"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Analytics"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Data Processing"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Analytics"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Search"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Analytics"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Streaming Analytics"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Analytics"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Analytics)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Analytics"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Productivity and Collaboration"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Business Applications"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Business Applications)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Business Applications"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Containers"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Compute"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "End User Computing"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Compute"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Quantum Compute"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Compute"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Serverless Compute"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Compute"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Virtual Machines"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Compute"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Compute)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Compute"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Caching"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Data Warehouses"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Ledger Databases"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "NoSQL Databases"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Relational Databases"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Time Series Databases"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Databases)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Databases"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Developer Platforms"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Developer Tools"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Continuous Integration and Deployment"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Developer Tools"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Development Environments"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Developer Tools"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Source Code Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Developer Tools"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Quality Assurance"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Developer Tools"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Developer Tools)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Developer Tools"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Identity and Access Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Identity"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Identity)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Identity"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "API Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Integration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Messaging"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Integration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Workflow Orchestration"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Integration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Integration)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Integration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "IoT Analytics"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Internet of Things"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "IoT Platforms"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Internet of Things"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Internet of Things)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Internet of Things"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Architecture"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Compliance"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Cost Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Data Governance"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Disaster Recovery"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Endpoint Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Observability"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Support"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Management and Governance)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Management and Governance"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Content Creation"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Media"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Gaming"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Media"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Media Streaming"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Media"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Mixed Reality"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Media"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Media)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Media"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Data Migration"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Migration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Resource Migration"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Migration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Migration)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Migration"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Mobile)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Mobile"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Multicloud Integration"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Multicloud"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Multicloud)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Multicloud"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Application Networking"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Content Delivery"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Network Connectivity"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Network Infrastructure"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Network Routing"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Network Security"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Networking)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Networking"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Secret Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Security"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Security Posture Management"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Security"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Threat Detection and Response"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Security"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Security)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Security"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Backup Storage"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Storage"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Block Storage"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Storage"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "File Storage"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Storage"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Object Storage"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Storage"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Storage Platforms"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Storage"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Storage)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Storage"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Application Platforms"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Web"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Web)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Web"
                        }
                        ]
                    },
                    {
                        "CheckFunction": "AND",
                        "Items": [
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceSubcategory",
                            "Value": "Other (Other)"
                        },
                        {
                            "CheckFunction": "CheckValue",
                            "ColumnName": "ServiceCategory",
                            "Value": "Other"
                        }
                        ]
                    }
                    ]
                },
                "Condition": {},
                "Dependencies": []
                }
            }
        }
        self.spec_rules = SpecRulesFromData(
            rule_data=self.rule_data,
            focus_dataset="CostAndUsage",
            filter_rules=None,
            applicability_criteria_list=["ALL"]
        )
        self.spec_rules.load()

    def test_rule_pass_scenario(self):
        """Test pass."""
        csv_data = """ServiceCategory,ServiceSubcategory
"AI and Machine Learning","AI Platforms"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ServiceSubcategory-C-004-M"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")

    def test_rule_pass_scenario2(self):
            """Test pass."""
            csv_data = """ServiceCategory,ServiceSubcategory
"Databases","Time Series Databases"
    """
            df = pd.read_csv(StringIO(csv_data))
            results = self.spec_rules.validate(focus_data=df)
            
            # Check rule state
            rule_result = results.by_rule_id["ServiceSubcategory-C-004-M"]
            self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")

    def test_rule_fail_scenario(self):
        """Test failure."""
        csv_data = """ServiceCategory,ServiceSubcategory
"AI and Machine Learning","NonExistingSubcategory"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["ServiceSubcategory-C-004-M"]
        self.assertFalse(rule_result.get("ok"), f"Rule should FAIL but got: {rule_result}")

if __name__ == '__main__':
    unittest.main()
