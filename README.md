# Setup Guildelines

## Prerequisites
- Docker should be installed on your system.

## Start Airflow

Run the following commands:

```bash
docker compose up airflow-init
docker compose up -d
```

## Access Airflow
- URL: [http://localhost:8080/](http://localhost:8080/)
- Username: `airflow`
- Password: `airflow`

## Access Metabase
- URL: [http://localhost:3000/](http://localhost:3000/)
- Credentials:
  - Host: `postgres`
  - User: `airflow`
  - Password: `airflow`
  - Port: `5432`
