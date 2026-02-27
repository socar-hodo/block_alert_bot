# Block Alert Bot

Sends Slack alerts when target block types have occupied running time >= 7 days.

## Files
- `block_alert_bot.py`: query + dedupe + Slack sender
- `sql/block_occupation_alert.sql`: BigQuery SQL template
- `requirements-block-alert.txt`: python deps

## Env vars
- `SLACK_WEBHOOK_URL`: Incoming webhook URL
- `BLOCK_ALERT_MIN_DAYS`: default `7`
- `TARGET_BLOCK_TYPES`: default `OUTAGE,CUSTOMER_SERVICE,MAINTENANCE,CAR_ACCIDENT`
- `BLOCK_ALERT_SQL_FILE`: default `./sql/block_occupation_alert.sql`
- `BLOCK_ALERT_STATE_FILE`: default `./.block_alert_state.json`
- `BLOCK_ALERT_DRY_RUN`: `true/false`
- `GOOGLE_APPLICATION_CREDENTIALS` (if needed in your runtime)

## Setup
```powershell
pip install -r requirements-block-alert.txt
```

## Run once
```powershell
$env:BLOCK_ALERT_DRY_RUN='true'
python .\block_alert_bot.py
```

## Production run
```powershell
$env:BLOCK_ALERT_DRY_RUN='false'
$env:SLACK_WEBHOOK_URL='https://hooks.slack.com/services/xxx/yyy/zzz'
python .\block_alert_bot.py
```

## Scheduler example (Windows Task Scheduler)
Run every 1 hour:
```powershell
python C:\Users\User\codex\block_alert_bot.py
```

## Important
The default SQL cannot guarantee exact mapping for
`OUTAGE/CUSTOMER_SERVICE/MAINTENANCE/CAR_ACCIDENT` in all environments.
Update `sql/block_occupation_alert.sql` typed CTE to your production block source
(or replace the whole SQL with your verified query) while keeping output columns.
