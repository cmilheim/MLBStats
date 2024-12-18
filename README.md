# MLB Stats Engine

## Overview

The MLB Stats Engine is a data pipeline that retrieves, processes, and stores Major League Baseball (MLB) team and player statistics. The pipeline uses Apache Airflow for orchestration, MongoDB for data storage, and RabbitMQ for message queuing.

## Repository Structure

```
├── .gitignore
├── airflow/
│   ├── airflow-webserver.pid
│   ├── airflow.cfg
│   ├── airflow.db-journal
│   ├── dags/
│   │   ├── __pycache__/
│   │   └── welcome_dag.py
│   ├── logs/
│   │   ├── dag_id=welcome_dag/
│   │   ├── dag_processor_manager/
│   │   └── scheduler/
│   ├── standalone_admin_password.txt
│   └── webserver_config.py
├── docker-compose.yml
├── dockerfile
├── main.log
├── mainLogger.log
├── README.md
├── rosterLogger.log
├── src/
│   ├── main.py
│   ├── roster.py
│   └── teams.py
├── teamLogger.log
└── teamsLogger.log
```

## Components

### Airflow

- **Configuration**: The Airflow configuration is defined in `airflow/airflow.cfg`.
- **Webserver Config**: The webserver configuration is defined in `airflow/webserver_config.py`.
- **DAGs**: The DAGs are located in the `airflow/dags/` directory. The main DAG is `welcome_dag.py`.

### Docker

- **Dockerfile**: The Dockerfile for building the Airflow image is located in the root directory.
- **Docker Compose**: The `docker-compose.yml` file defines the services for MongoDB, RabbitMQ, MySQL, and Airflow.

### Source Code

- **Main Script**: The main script `src/main.py` retrieves MLB teams and sends them to RabbitMQ.
- **Teams Script**: The `src/teams.py` script processes team data from RabbitMQ and stores it in MongoDB.
- **Roster Script**: The `src/roster.py` script processes player roster data from RabbitMQ and stores it in MongoDB.

## Setup

1. **Clone the repository**:
    ```sh
    git clone <repository-url>
    cd MLBStats
    ```

2. **Build and start the Docker containers**:
    ```sh
    docker-compose up --build
    ```

3. **Access the Airflow web interface**:
    Open your browser and go to `http://localhost:8080`.

## Usage

1. **Trigger the DAG**:
    - In the Airflow web interface, trigger the `welcome_dag`.

2. **Monitor the pipeline**:
    - Monitor the tasks in the Airflow web interface to ensure they are running successfully.

## Logs

- **Main Logs**: `mainLogger.log`
- **Teams Logs**: `teamsLogger.log`
- **Roster Logs**: `rosterLogger.log`

## Configuration

### Airflow Configuration

- **DAGs Folder**: `/opt/airflow/dags`
- **Executor**: `SequentialExecutor`
- **Database**: `sqlite:////opt/airflow/airflow.db`

### Docker Compose Configuration

- **MongoDB**: 
    - Username: `root`
    - Password: `rootpassword`
    - Port: `27017`
- **RabbitMQ**:
    - Username: `cmilheim`
    - Password: `rmqpassword`
    - Ports: `5672`, `15672`
- **MySQL**:
    - Username: `cmilheim`
    - Password: `userpassword`
    - Port: `3306`

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.