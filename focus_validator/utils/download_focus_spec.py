import os
import requests


def downloadFocusSpec(version: str, ruleSetPath: str) -> bool:
    if not version:
        raise ValueError("Version is required for downloading FOCUS spec")

    url = f"https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec/releases/download/v{version}/FOCUS-spec-{version}.json"

    # Create version directory if it doesn't exist
    versionDir = os.path.join(ruleSetPath, version)
    os.makedirs(versionDir, exist_ok=True)

    # Download file
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Save to the expected location
        filePath = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'rules', f"cr-{version}.json")
        with open(filePath, 'w', encoding='utf-8') as f:
            f.write(response.text)

        print(f"Successfully downloaded FOCUS spec {version} to {filePath}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"Failed to download FOCUS spec {version}: {e}")
        return False
