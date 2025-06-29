# Airflow DAG Scripts

A collection of example Apache Airflow DAGs and monitoring setup.

## 🚀 Getting Started

### 1. Install Apache Airflow

We recommend using a virtual environment.

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install --upgrade pip
pip install apache-airflow
```

Or, for a specific Airflow version:

```bash
pip install "apache-airflow==2.7.3"
```

> For MySQL/Postgres support, add the relevant extras, e.g.:
> `pip install "apache-airflow[mysql]==2.7.3"`

### 2. Initialize Airflow

```bash
airflow db init
```

### 3. Start Airflow (Standalone)

For quick testing, use:

```bash
airflow standalone
```

This will start the webserver and scheduler, and create an admin user (shown in the output).

- Web UI: http://localhost:8080
- Default user: `admin` / password shown in terminal

### 4. Add Example DAGs

Copy or symlink the example DAG `.py` files into your Airflow `dags/` folder (default: `~/airflow/dags/`).

```bash
cp Example_*/code.py ~/airflow/dags/
```

Or set the `AIRFLOW__CORE__DAGS_FOLDER` environment variable to point to this repo.

### 5. Start/Stop Airflow Services (Advanced)

To run components separately:

```bash
airflow webserver --port 8080
airflow scheduler
```

## 🛠️ Useful Airflow CLI Commands

- List all DAGs:
  ```bash
  airflow dags list
  ```
- List all tasks in a DAG:
  ```bash
  airflow tasks list <dag_id>
  ```
- Trigger a DAG run manually:
  ```bash
  airflow dags trigger <dag_id>
  ```
- Pause/Unpause a DAG:
  ```bash
  airflow dags pause <dag_id>
  airflow dags unpause <dag_id>
  ```
- Show DAG structure as ASCII:
  ```bash
  airflow dags show <dag_id>
  ```
- Test a task (does not affect DB state):
  ```bash
  airflow tasks test <dag_id> <task_id> <execution_date>
  ```
- Re-serialize all DAGs (useful for troubleshooting DAG parsing issues):
  ```bash
  airflow dags reserialize
  ```
- Clear task instances:
  ```bash
  airflow tasks clear <dag_id> --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD>
  ```

## 📊 Monitoring Stack (Optional)

To enable metrics and dashboards:

1. Go to the `monitoring/` directory:

    ```bash
    cd monitoring
    ```

2. Start Prometheus, Grafana, and StatsD exporter:

    ```bash
    docker-compose up -d
    ```

3. Enable Airflow metrics in `airflow.cfg`:

    ```
    [metrics]
    statsd_on = True
    statsd_host = localhost
    statsd_port = 8125
    statsd_prefix = airflow
    ```

4. Restart Airflow.

- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

## 🧹 Cleanup

To stop Airflow:

```bash
# If using standalone
pkill -f airflow

# Or stop webserver/scheduler individually
airflow webserver --stop
airflow scheduler --stop
```

To stop monitoring stack:

```bash
cd monitoring
docker-compose down -v
```

---

For more details, see the comments in each DAG file.
