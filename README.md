# Home Lab Data Warehouse & ETL Platform

A self-hosted data engineering platform for ingesting, orchestrating, transforming, and maintaining data across a home lab environment.

This project brings together **Docker, Apache Airflow, PostgreSQL, and dbt** to provide a centralized analytics warehouse and an automated ETL/ELT pipeline framework for data originating from various home lab servers, applications, and services.

The goal is to create a maintainable, modular, and reproducible data platform that can be extended as new data sources, services, and analytical requirements are introduced.

---

## Overview

The platform provides an end-to-end workflow for managing data throughout its lifecycle:

* **Data ingestion:** Collect data from home lab servers, applications, databases, and services.
* **Orchestration:** Schedule, coordinate, and monitor data pipelines using Apache Airflow.
* **Data warehousing:** Store and organize ingested data in a centralized PostgreSQL warehouse.
* **Data transformation:** Use dbt to implement SQL-based transformations, modeling, and data quality checks.
* **Infrastructure management:** Run the platform components in isolated, reproducible Docker containers.
* **Warehouse maintenance:** Support ongoing data updates, transformation runs, and maintenance workflows.

The architecture is designed to keep ingestion, orchestration, storage, and transformation concerns modular while allowing them to work together as a cohesive data platform.

## Architecture

<pre>
 Home Lab Servers & Services
              |
              v
      Data Ingestion
   (ETL / ELT Pipelines)
              |
              v
      Apache Airflow
      Orchestration
              |
              v
       PostgreSQL
      Data Warehouse
              |
              v
          dbt Core
   Transformations & Models
              |
              v
      Analytics-Ready
          Datasets
</pre>

All core platform components run in Docker containers and are managed through Docker Compose.

Airflow coordinates pipeline execution, PostgreSQL provides centralized warehouse storage, and dbt manages SQL transformations and data modeling.

The exact execution order and dependencies are defined by the individual pipelines and project configuration.

## Technology Stack

| Technology     | Purpose                                                 |
| -------------- | ------------------------------------------------------- |
| Docker         | Containerized execution environment                     |
| Docker Compose | Multi-service infrastructure management                 |
| Apache Airflow | Workflow scheduling, orchestration, and monitoring      |
| PostgreSQL     | Centralized data warehouse                              |
| dbt            | SQL transformations, data modeling, and testing         |
| Python         | Pipeline logic, integrations, and supporting automation |
| Git / GitHub   | Version control and project documentation               |

### Component responsibilities

**`airflow/`**

Contains the Airflow configuration, DAG definitions, plugins, and supporting files used to orchestrate ETL/ELT workflows.

**`dbt/`**

Contains the dbt project, including transformation models, macros, tests, seeds, and other resources used to build and maintain warehouse datasets.

**`postgres/`**

Contains supporting configuration and initialization resources for the PostgreSQL warehouse, where applicable.

**`docker-compose.yml`**

Defines the containerized services, networking, volumes, and dependencies required to run the platform.

**`scripts/`**

Contains supporting automation and utility scripts, where applicable.

## Getting Started

### Prerequisites

Ensure the host system has the following installed:

* Docker Engine or Docker Desktop
* Docker Compose v2
* Git
* Sufficient disk space for persistent warehouse data and Airflow logs

Depending on the configuration, additional requirements may include access to source systems, database credentials, API tokens, and network connectivity to home lab services.

### 1. Clone the repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd <YOUR_REPOSITORY_DIRECTORY>
```

### 2. Configure environment variables

Create a local environment file using the provided example, if available.

```bash
cp .env.example .env
```

Update the configuration with the appropriate values for your environment.

Typical configuration includes:

* PostgreSQL database name, username, and password
* Airflow configuration and database connection details
* dbt connection settings
* Source system connection strings and credentials
* API tokens and other integration-specific secrets

**Do not commit `.env` files, passwords, API tokens, or other sensitive credentials to GitHub.**

Use the repository's existing environment variable names and configuration conventions.

### 3. Start the platform

From the repository root, start the Docker Compose services:

```bash
docker compose up -d
```

Check the status of the running containers:

```bash
docker compose ps
```

Review service logs when troubleshooting startup issues:

```bash
docker compose logs -f
```

To inspect a specific service:

```bash
docker compose logs -f <SERVICE_NAME>
```

Replace `<SERVICE_NAME>` with the relevant Compose service name.

### 4. Access the services

Once the containers are running, access the platform through the ports configured in `docker-compose.yml`.

| Service        | Access                                          |
| -------------- | ----------------------------------------------- |
| Apache Airflow | `http://localhost:<AIRFLOW_PORT>`               |
| PostgreSQL     | `localhost:<POSTGRES_PORT>`                     |
| dbt            | Executed through the configured dbt environment |

PostgreSQL is a database service rather than a web application, so access it using an appropriate PostgreSQL client.

The actual ports, credentials, and service names depend on the local Compose configuration.

## Data Pipelines & Orchestration

Apache Airflow is responsible for coordinating the execution of the platform's data pipelines.

Pipelines can be defined to handle different sources, destinations, schedules, and processing requirements across the home lab.

Typical workflow responsibilities include:

1. Connecting to a source system or service.
2. Extracting the required data.
3. Loading or updating data in the PostgreSQL warehouse.
4. Executing downstream dbt transformations.
5. Validating pipeline execution and handling failures.

Airflow DAGs define task dependencies, scheduling behavior, retries, and execution order.

This allows individual pipelines to be maintained independently while supporting coordinated workflows across the broader data platform.

### Pipeline design

The platform is intended to support multiple independent data sources and workflows.

Examples of potential integrations include:

* Home lab infrastructure and server metrics
* Self-hosted applications and services
* Application databases
* System inventories and operational data
* APIs and other external or internal data sources

The actual sources and integrations are determined by the DAGs and connectors implemented in the repository.

## Data Warehouse

PostgreSQL serves as the centralized data warehouse for the platform.

It provides persistent storage for ingested data and transformed datasets, allowing information from different home lab systems to be consolidated and modeled for downstream use.

The warehouse is managed as part of the Dockerized infrastructure, with persistent storage configured through Docker volumes or the applicable storage configuration.

### Warehouse responsibilities

* Centralized storage of data from multiple sources
* Organization of datasets into appropriate schemas and tables
* Support for incremental data updates and recurring ingestion
* Storage of dbt-managed transformation outputs
* Availability of structured datasets for analytics and reporting

Database initialization, schema organization, retention, backup, and recovery behavior depend on the corresponding PostgreSQL and project configuration.

## Data Transformations with dbt

dbt is used to implement and maintain the transformation layer of the warehouse.

Rather than embedding all transformation logic directly into ingestion workflows, dbt provides a dedicated project for defining SQL models, managing dependencies, and validating data.

### Transformation capabilities

* SQL-based data modeling
* Layered transformations between raw, intermediate, and analytical datasets
* Reusable macros and shared transformation logic
* Model dependency management
* Data quality tests and validation
* Documentation of models and columns
* Incremental models and snapshots, where implemented

### Typical dbt workflow

```bash
dbt debug
dbt deps
dbt run
dbt test
```

These commands should be executed from the configured dbt project directory and within the appropriate container or virtual environment.

For a full transformation and validation run:

```bash
dbt build
```

The exact command, target, profile, and execution environment should match the project's dbt configuration.

dbt can also be incorporated into Airflow DAGs so that transformations execute automatically after the required ingestion tasks complete.

## Operating the Platform

### Start services

```bash
docker compose up -d
```

### Stop services

```bash
docker compose down
```

### Restart services

```bash
docker compose restart
```

### View running containers

```bash
docker compose ps
```

### Follow logs

```bash
docker compose logs -f
```

### Rebuild containers after configuration changes

```bash
docker compose up -d --build
```

**Note:** `docker compose down` normally preserves named volumes, but removing volumes or using destructive cleanup commands can delete persistent database data. Review the Compose configuration before performing cleanup.

## Development & Maintenance

The project is designed to evolve as new sources, transformations, and operational requirements are introduced.

### Adding a new data source

A typical integration workflow is:

1. Identify the source system and required data.
2. Implement the extraction and loading logic.
3. Configure the required credentials and connections.
4. Add or update the relevant Airflow DAG.
5. Create or update dbt models for the ingested data.
6. Add appropriate tests and validation.
7. Run the pipeline and verify the resulting warehouse datasets.

### Updating transformations

When modifying dbt models:

1. Update the relevant SQL model or macro.
2. Review downstream model dependencies.
3. Execute the applicable dbt transformations.
4. Run the associated tests.
5. Verify the resulting warehouse data.

### Infrastructure changes

Changes to container configuration, service dependencies, environment variables, and persistent storage should be made through the appropriate Docker Compose and component configuration files.

Where practical, validate changes in a development environment before applying them to the running warehouse.

## Data Persistence & Backups

The PostgreSQL warehouse contains persistent project data and should be treated as stateful infrastructure.

A production-ready deployment should account for:

* Persistent database storage
* Regular PostgreSQL backups
* Backup retention and storage location
* Restore procedures and periodic recovery testing
* Protection of credentials and configuration secrets
* Monitoring of disk usage and database growth

Airflow metadata, logs, and other service data may also require persistence depending on the deployment configuration.

Refer to the actual Compose volumes and database configuration to determine which data is persisted and how it should be backed up.

## Security Considerations

This project is designed for a self-hosted environment. Appropriate access controls are important, especially when services are reachable from other systems on the home lab network.

Recommended practices include:

* Keep secrets out of version control.
* Use dedicated credentials for individual services and integrations.
* Avoid exposing PostgreSQL directly to untrusted networks.
* Restrict access to the Airflow web interface.
* Use appropriate network segmentation and firewall rules.
* Keep container images and dependencies updated.
* Review permissions on mounted host directories and Docker volumes.
* Back up important configuration and warehouse data securely.

Actual security controls depend on the deployment and network configuration.

## Troubleshooting

| Issue                        | What to check                                                                       |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| Containers fail to start     | Compose configuration, container logs, port conflicts                               |
| Airflow DAGs are missing     | DAG mount paths, parsing errors, scheduler logs                                     |
| Airflow tasks fail           | Task logs, connection settings, credentials, network access                         |
| PostgreSQL connection errors | Hostname, port, database credentials, Docker networking                             |
| dbt connection errors        | Active profile, target, environment variables, database permissions                 |
| Transformations fail         | SQL errors, model dependencies, schema and source availability                      |
| Data is missing              | Ingestion task status, source connectivity, load logic, transformation dependencies |
| Storage usage increases      | Database growth, retained logs, Docker volumes                                      |

For service-specific logs:

```bash
docker compose logs -f <SERVICE_NAME>
```

For container status:

```bash
docker compose ps
```

## Project Goals

The long-term objective is to maintain a flexible and extensible home lab data platform that supports:

* Centralized data collection across multiple systems
* Reliable and repeatable ETL/ELT execution
* Automated orchestration and scheduling
* Structured, maintainable warehouse models
* Data quality validation
* Simplified infrastructure deployment and maintenance
* Incremental expansion as new services and data sources are introduced

## Future Enhancements

Potential areas for future development include:

* Expanded source integrations
* More comprehensive dbt model and column documentation
* Automated data quality reporting
* Improved pipeline failure notifications
* Warehouse monitoring and performance optimization
* Automated database backup and restore workflows
* CI/CD validation for DAGs and dbt models
* Infrastructure health dashboards
* Data lineage and pipeline observability

These are potential extensions rather than a representation of features already implemented.

## License

Specify the license for this project here.

If no license has been added, all rights remain with the copyright holder by default.

---

*Built to bring the home lab's data together into a centralized, automated, and maintainable analytics platform.*
