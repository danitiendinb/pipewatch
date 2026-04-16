# pipewatch

Lightweight CLI monitor for ETL pipeline health with alerting hooks.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/youruser/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Monitor a pipeline by pointing pipewatch at your config file:

```bash
pipewatch monitor --config pipelines.yaml
```

Example `pipelines.yaml`:

```yaml
pipelines:
  - name: daily_sales_etl
    check_interval: 60
    alert_on_failure: true
    hooks:
      slack: "https://hooks.slack.com/services/your/webhook/url"
```

Run a one-time health check:

```bash
pipewatch check --pipeline daily_sales_etl
```

View pipeline status in the terminal:

```bash
pipewatch status
```

```
Pipeline           Status     Last Run         Duration
-----------------  ---------  ---------------  --------
daily_sales_etl    ✓ healthy  2 minutes ago    4.2s
user_sync          ✗ failed   15 minutes ago   --
```

---

## Features

- Real-time pipeline health monitoring via CLI
- Configurable alerting hooks (Slack, webhooks, email)
- Simple YAML-based pipeline definitions
- Lightweight with minimal dependencies

---

## License

MIT © 2024 pipewatch contributors