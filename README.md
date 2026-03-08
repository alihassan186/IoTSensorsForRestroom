# MQTT Restroom Monitoring Project

A Python-based restroom monitoring project that simulates sensor data, stores telemetry in MySQL, and creates MongoDB notifications for both:

- normal alerts (`alerts` collection)
- rule-engine alerts (`rules` / `restroomRules` / `rest-room rules` collection)

## Project Components

- `Restroomcode.py` — sensor simulator + MySQL writer + MongoDB alert/rule notification engine
- `FastAPICode.py` — sample FastAPI Todo app (independent from simulator)
- `cleardb.py` — DB utility script for table changes
- `ALERT_TESTING_GUIDE.md` — alert testing scenarios

## Features

- Real-time sensor simulation for multiple restroom sensor types
- Per-owner custom database routing support
- MongoDB Change Stream listener for dynamic reloads
- Alert evaluation from `alerts` collection
- Rule engine evaluation from rules collection
- Notification creation in `notifications` collection
- Notification classification with `type: "ruleengine"` for rule-engine notifications

## Tech Stack

- Python 3.12+
- MongoDB Atlas (`pymongo`)
- MySQL (`pymysql`)
- FastAPI (`fastapi`, `uvicorn`) for the API example script

## Quick Start

### 1) Clone and enter project

```bash
git clone <your-repo-url>
cd MQTTCode
```

### 2) Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3) Install dependencies

```bash
pip install -r requirements.txt
```

### 4) Configure environment

Copy `.env.example` to `.env` and update values.

> Current runtime scripts still use in-file constants. `.env` is provided as a GitHub-standard configuration baseline for future migration.

### 5) Run simulator

```bash
python3 Restroomcode.py
```

### 6) Optional: run FastAPI example app

```bash
python3 FastAPICode.py
```

## MongoDB Collections Used

- `sensors` — connected sensor definitions (`isConnected: true`)
- `restrooms` — restroom metadata (`numOfToilets`)
- `auths` — owner DB routing config
- `alerts` — normal alert definitions
- `notifications` — generated notifications
- rules collection candidates (auto-detected):
  - `rules`
  - `restroomRules`
  - `rest-room rules`

## Notification Types

- Normal alert notification: existing alert flow
- Rule-engine notification: includes `"type": "ruleengine"`

## Development Commands

```bash
# Syntax check
python3 -m py_compile Restroomcode.py

# Run simulator
python3 Restroomcode.py
```

## Security Notes

- Never commit real secrets to Git.
- Rotate credentials before publishing this repository.
- Use `.env`/secret manager for production deployments.

See `SECURITY.md` for reporting and hardening guidance.

## Contributing

Please read `CONTRIBUTING.md` before opening issues or pull requests.

## License

This project is licensed under the MIT License. See `LICENSE`.
