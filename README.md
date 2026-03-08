# Hello World Workspace

A Microsoft Fabric workspace with a simple Hello World PySpark notebook.

## Workspace Items

| Item | Type | Purpose |
|------|------|---------|
| **HelloWorld** | Notebook | Prints "Hello, World!" using PySpark |

## Deployment

Artifacts are deployed via `fabric-cicd`. Environment-specific workspace IDs are parameterised in `config/parameter.yml`.

```
DEV  workspace: 334687d9-c5a3-4af6-a9b1-9c02bad79934
QA   workspace: 9b6c012c-1c20-4807-9feb-a7703a7671bd
PROD workspace: 7d836180-43af-494d-80e9-de50ed5f8529
```

## Usage

1. Deploy the workspace using `deploy.ps1`
2. Run **HelloWorld** notebook to verify the environment
