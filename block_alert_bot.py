import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests
from google.cloud import bigquery


DEFAULT_TARGET_BLOCK_TYPES = [
    "OUTAGE",
    "CUSTOMER_SERVICE",
    "MAINTENANCE",
    "CAR_ACCIDENT",
]


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_state(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        return {"sent_keys": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"sent_keys": []}


def save_state(path: Path, keys: List[str]) -> None:
    payload = {
        "sent_keys": keys,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_sql(sql_file: Path) -> str:
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    return sql_file.read_text(encoding="utf-8")


def query_blocks(sql: str, min_days: int, target_block_types: List[str]) -> List[dict]:
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("min_days", "INT64", min_days),
            bigquery.ArrayQueryParameter("target_block_types", "STRING", target_block_types),
        ]
    )
    rows = client.query(sql, job_config=job_config).result()
    return [dict(row.items()) for row in rows]


def format_slack_text(rows: List[dict], min_days: int) -> str:
    lines = [
        f":rotating_light: BLOCK alert - occupied >= {min_days} days",
        f"count: {len(rows)}",
        "",
        "top rows:",
    ]
    for row in rows[:30]:
        lines.append(
            "- "
            f"type={row.get('block_type', 'UNKNOWN')} | "
            f"car_id={row.get('car_id')} | "
            f"days={row.get('occupied_days')} | "
            f"start={row.get('start_at_utc')} | "
            f"end={row.get('end_at_utc') or 'NULL'} | "
            f"key={row.get('block_key')}"
        )
    if len(rows) > 30:
        lines.append(f"... and {len(rows) - 30} more")
    return "\n".join(lines)


def send_slack(webhook_url: str, text: str) -> None:
    response = requests.post(webhook_url, json={"text": text}, timeout=15)
    response.raise_for_status()


def main() -> None:
    min_days = int(os.getenv("BLOCK_ALERT_MIN_DAYS", "7"))
    target_types_raw = os.getenv("TARGET_BLOCK_TYPES", ",".join(DEFAULT_TARGET_BLOCK_TYPES))
    target_block_types = [x.strip() for x in target_types_raw.split(",") if x.strip()]

    sql_file = Path(os.getenv("BLOCK_ALERT_SQL_FILE", "./sql/block_occupation_alert.sql")).resolve()
    state_file = Path(os.getenv("BLOCK_ALERT_STATE_FILE", "./.block_alert_state.json")).resolve()
    dry_run = env_bool("BLOCK_ALERT_DRY_RUN", False)

    sql = load_sql(sql_file)
    rows = query_blocks(sql, min_days=min_days, target_block_types=target_block_types)

    prev_state = load_state(state_file)
    prev_keys = set(prev_state.get("sent_keys", []))

    current_keys = [str(r.get("block_key")) for r in rows if r.get("block_key") is not None]
    new_rows = [r for r in rows if str(r.get("block_key")) not in prev_keys]

    if not new_rows:
        print(f"No new alerts. active_rows={len(rows)}")
        save_state(state_file, current_keys)
        return

    message = format_slack_text(new_rows, min_days=min_days)

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if dry_run or not webhook_url:
        print("[DRY RUN] Slack message")
        print(message)
    else:
        send_slack(webhook_url, message)
        print(f"Sent Slack alert. new_rows={len(new_rows)}")

    save_state(state_file, current_keys)


if __name__ == "__main__":
    main()
