# REHAT Command Center

Internal hotel revenue management system. Multi-property, powered by Exely PMS API.

## Directory Structure

```
rehat/
├── app.py                  # Streamlit entry point
├── config.py               # PropertyConfig dataclass + scheduler settings
├── db.py                   # SQLite schema, migrations, connection helpers
├── scheduler.py            # Background APScheduler daemon
├── ingestion/
│   ├── exely_client.py     # Exely API wrapper
│   ├── services.py         # /analytics/services → raw_services + daily_snapshot
│   └── bookings.py         # /bookings → bookings_on_books + bob_snapshots
├── modules/
│   ├── property_kpis.py    # Per-property KPI dashboard
│   ├── portfolio.py        # Cross-property analytics
│   ├── budgeting.py        # Budget input + tracking
│   ├── pnl.py              # P&L per property
│   ├── company_financials.py  # REHAT consolidated financials
│   ├── acquisition.py      # Hotel acquisition modeler
│   └── settings.py         # Property config UI
└── .streamlit/config.toml  # Dark theme + server settings
```

## Running Locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

Default password: `rehat2024` (change via env var below)

## Deploying 24/7 (Docker)

**Prerequisites:** Any Linux VPS with Docker installed (DigitalOcean, Hetzner, AWS EC2, etc.)

```bash
# 1. Clone / upload repo to server
git clone <your-repo> /opt/rehat
cd /opt/rehat

# 2. Set your password
echo "REHAT_PASSWORD=your_secure_password" > .env

# 3. Start
docker compose up -d

# App is now live at http://your-server-ip:8501
# Restarts automatically on crash or server reboot
```

**To view logs:**
```bash
docker compose logs -f
```

**To update:**
```bash
git pull
docker compose up -d --build
```

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `REHAT_PASSWORD` | `rehat2024` | Login password for all users |
| `REHAT_DB_PATH` | `./rehat.db` | SQLite DB path (set to `/data/rehat.db` in Docker) |

## Making It HTTPS / Domain

Recommended: put Nginx reverse proxy + Let's Encrypt in front.

```nginx
server {
    listen 443 ssl;
    server_name command.rehat.id;  # your domain

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

## Adding a New Property

1. Open **Property Config** in the sidebar
2. Fill in property details + Exely API key
3. Save — scheduler picks it up on the next tick (within 5 minutes)

## Data Flow

```
Exely API → ingestion/services.py → raw_services → daily_snapshot
           → ingestion/bookings.py → bookings_on_books → bob_snapshots
Scheduler runs every 5 min, lookback 2 days (catches updates/corrections)
```
