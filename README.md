# FY26SDKChallenge — Hello World Notebook

## Overview
This workspace contains a simple **Hello World** Fabric notebook demonstrating basic Fabric Git integration and CI/CD deployment using `fabric-cicd`.

## Workspace Contents
- **workspace/HelloWorld.Notebook** — A minimal PySpark notebook that prints `Hello, World!`

## Environments
Deployments target three environments: **DEV**, **QA**, and **PROD**, with workspace IDs configured in `config/variable.json` and parameterized via `config/parameter.yml`.

## Deployment
Run `deploy.ps1` to deploy workspace items to the target environment using `fabric-cicd`.