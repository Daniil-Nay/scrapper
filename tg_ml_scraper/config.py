from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    session_name: str
    channels: list[str]
    lookback_days: int
    schedule_hour: int
    schedule_minute: int
    db_path: Path
    bot_token: str | None


def _parse_channels(raw: str) -> list[str]:
    channels = [c.strip().lstrip("@") for c in raw.split(",") if c.strip()]
    if not channels:
        raise ValueError("список каналов пуст=(")
    return channels


def load_settings() -> Settings:
    load_dotenv()

    api_id_raw = os.getenv("TG_API_ID", "").strip()
    api_hash = os.getenv("TG_API_HASH", "").strip()

    if not api_id_raw or not api_hash:
        raise ValueError("tg_adi & tg_api заполни!!")

    channels = _parse_channels(os.getenv("TG_CHANNELS", ""))
    db_path = Path(os.getenv("DB_PATH", "data/scraper.db"))

    return Settings(
        api_id=int(api_id_raw),
        api_hash=api_hash,
        session_name=os.getenv("TG_SESSION_NAME", "ml_ds_scraper").strip(),
        channels=channels,
        lookback_days=int(os.getenv("LOOKBACK_DAYS", "7")),
        schedule_hour=int(os.getenv("SCHEDULE_HOUR", "9")),
        schedule_minute=int(os.getenv("SCHEDULE_MINUTE", "0")),
        db_path=db_path,
        bot_token=(os.getenv("BOT_TOKEN", "").strip() or None),
    )
