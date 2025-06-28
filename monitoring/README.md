# Airflow Monitoring Stack

Complete monitoring setup for Apache Airflow with Prometheus and Grafana.

## 🚀 Quick Start

```bash
# 1. Start monitoring stack
cd monitoring
docker-compose up -d

# 2. Configure Airflow metrics (add to ~/airflow/airflow.cfg)
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow

# 3. Restart Airflow to enable metrics
```

## 📊 Access

- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Prometheus Metrics**: http://localhost:9090
- **Raw Metrics**: http://localhost:9102/metrics

## 🏗️ How It Works

```
Airflow → StatsD (8125) → Prometheus (9090) → Grafana (3000)
```

1. **Airflow** sends metrics via StatsD protocol
2. **StatsD Exporter** converts metrics to Prometheus format
3. **Prometheus** stores time-series data
4. **Grafana** displays beautiful dashboards

## 📈 Dashboard Features

- **DAG Count**: Total number of DAGs loaded
- **Import Errors**: DAGs with parsing errors
- **Executor Status**: Available task slots
- **Task Pipeline**: Queued vs running tasks
- **Real-time Updates**: 30-second refresh

## 🔧 Files Included

- `docker-compose.yml` - Main orchestrator (StatsD + Prometheus + Grafana)
- `prometheus.yml` - Prometheus configuration
- `airflow_dashboard.json` - Pre-built Grafana dashboard
- `dashboard-provider.yml` - Auto-loads dashboard
- `datasource.yml` - Auto-connects Prometheus

## 📋 Requirements

- Docker & Docker Compose
- Apache Airflow 3.0+
- 4GB+ RAM recommended

## 🎯 Expected Metrics

After running DAGs, you'll see:

- Task success/failure rates
- DAG execution times
- Scheduler performance
- Pool utilization

## 🛠️ Troubleshooting

**No data in Grafana?**

```bash
# Check if metrics are flowing
curl http://localhost:9102/metrics | grep airflow

# Verify Airflow config
grep -A5 "\[metrics\]" ~/airflow/airflow.cfg
```

**Dashboard not loading?**

```bash
# Restart Grafana
docker-compose restart grafana
```

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove data volumes
docker-compose down -v
```
