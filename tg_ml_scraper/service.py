from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, UsernameInvalidError, UsernameNotOccupiedError

from tg_ml_scraper.config import Settings
from tg_ml_scraper.db import ensure_db, open_db, replace_links, upsert_post, upsert_snapshot
from tg_ml_scraper.extractors import extract_reactions, extract_urls, links_with_types, total_reactions


logger = logging.getLogger(__name__)


@dataclass
class ScrapeStats:
    channels_total: int = 0
    channels_ok: int = 0
    channels_failed: int = 0
    posts_processed: int = 0


def _normalize_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class TelegramChannelScraper:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def scrape_lookback(self, lookback_days: int | None = None) -> ScrapeStats:
        ensure_db(self.settings.db_path)
        effective_lookback = lookback_days or self.settings.lookback_days
        since_dt = datetime.now(timezone.utc) - timedelta(days=effective_lookback)
        snapshot_day = datetime.now(timezone.utc).date()

        stats = ScrapeStats(channels_total=len(self.settings.channels))
        client = TelegramClient(
            self.settings.session_name,
            self.settings.api_id,
            self.settings.api_hash,
        )
        await client.start()
        logger.info("Telegram client started. Lookback window: %s days", effective_lookback)

        try:
            with open_db(self.settings.db_path) as conn:
                for raw_channel in self.settings.channels:
                    channel = raw_channel.lstrip("@")
                    try:
                        entity = await client.get_entity(channel)
                    except (UsernameInvalidError, UsernameNotOccupiedError, ChannelPrivateError) as exc:
                        stats.channels_failed += 1
                        logger.warning("Skip channel %s: %s", channel, exc)
                        continue
                    except Exception as exc:  # noqa: BLE001
                        stats.channels_failed += 1
                        logger.exception("Unexpected error in channel %s: %s", channel, exc)
                        continue

                    channel_title = getattr(entity, "title", channel)
                    channel_username = getattr(entity, "username", None)
                    channel_id = int(getattr(entity, "id"))
                    posts_in_channel = 0

                    async for message in client.iter_messages(entity):
                        if message.date is None:
                            continue
                        msg_dt = _normalize_dt(message.date)
                        if msg_dt < since_dt:
                            break

                        urls = extract_urls(message)
                        reactions_map = extract_reactions(message)
                        post_url = (
                            f"https://t.me/{channel_username}/{message.id}"
                            if channel_username
                            else None
                        )

                        post_id = upsert_post(
                            conn,
                            channel_id=channel_id,
                            channel_username=channel_username,
                            channel_title=channel_title,
                            message_id=message.id,
                            post_url=post_url,
                            post_datetime=msg_dt.isoformat(),
                            message_text=message.message,
                            views=message.views,
                            forwards=message.forwards,
                        )
                        replace_links(conn, post_id, links_with_types(urls))
                        upsert_snapshot(
                            conn,
                            post_id=post_id,
                            snapshot_date=snapshot_day,
                            total_reactions=total_reactions(reactions_map),
                            reactions=reactions_map,
                            views=message.views,
                            forwards=message.forwards,
                        )
                        posts_in_channel += 1

                    stats.channels_ok += 1
                    stats.posts_processed += posts_in_channel
                    logger.info(
                        "Channel %s (%s): %s posts processed",
                        channel_title,
                        channel,
                        posts_in_channel,
                    )
                conn.commit()
        finally:
            await client.disconnect()

        return stats

