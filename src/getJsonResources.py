# Simple python script to extract resources from the API and save them in JSON files

import requests
import json

resourceTypes = [
    "AdverseEvent",
    "CarePlan",
    "Condition",
    "Consent",
    "Device",
    "FamilyMemberHistory",
    "MedicationStatement",
    "MolecularSequence",
    "Observation",
    "Organization",
    "Patient",
    "Procedure",
    "Questionnaire",
    "QuestionnaireResponse",
    "ResearchStudy",
    "ResearchSubject",
    "Specimen",
    "VerificationResult",
]

for i in resourceTypes:
    url = "https://xxx/api/" + i + "?_tag=xxx"
    headers = {
        "Authorization": "Bearer xxx" # Arkhn access token
    }

    r = requests.get(url, headers=headers)
    entries = r.json()["entry"]
    if len(entries) > 0:
        for entry in entries:
            filePath = (
                "resources/"
                + entry["resource"]["resourceType"]
                + "_"
                + entry["resource"]["id"]
                + ".json"
            )
            with open(filePath, "w") as outfile:
                json.dump(entry["resource"], outfile)
            print(entry["resource"]["id"])