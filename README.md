# ops-tools

## Repository structure

This repo gathers general documents and resources used for operations.

```

    .
    ├── apps/                     # indications for app developers
        └── elements/             # FHIR attributes to display
        └── searchqueries/        # queries in FHIR search syntax
    └── sourceDB/                 # doc and queries abouts source DB
        ├── arkhndoc/             # homemade doc about db
        └── codes/                # codes present in source db
        ├── hospitaldoc/          # doc provided by editor or hospital
        ├── schemas/              # schemaspy outputs, db schema
        └── SQLqueries/           # documented SQL queries
    └── fhirDB/                   # Additional fhir resources relevant to understand fhirDB structure
        ├── codesystems/
        ├── examples/
        ├── extensions/
        ├── implementationguide/  # input files needed to generate IG
        ├── profiles/             # used profiles
        └── valuesets/            # used valuesets
    └── mappings/                 # mappings and additional resources

```
