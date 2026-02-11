from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from tg_ml_scraper.config import load_settings
from tg_ml_scraper.db import ensure_db
from tg_ml_scraper.reporting import (
    export_top_posts_json,
    export_top_posts_markdown,
    get_top_posts,
)
from tg_ml_scraper.service import TelegramChannelScraper


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(errors="replace")
        except Exception:  
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Telegram ML/DS channel scraper")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logs")

    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_parser = subparsers.add_parser("scrape-once", help="Scrape channels once")
    scrape_parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override LOOKBACK_DAYS for this run",
    )

    subparsers.add_parser("run-daily", help="Run scheduler + scrape once at startup")
    subparsers.add_parser("run-bot", help="Run Telegram bot interface (aiogram)")

    report_parser = subparsers.add_parser("report-top", help="Print top posts with github links")
    report_parser.add_argument("--limit", type=int, default=20, help="Max posts in report")
    report_parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override LOOKBACK_DAYS for report",
    )
    report_parser.add_argument(
        "--show-links",
        action="store_true",
        help="Print article/research/github URLs for each post",
    )

    export_parser = subparsers.add_parser("export-weekly", help="Export top github-linked posts to files")
    export_parser.add_argument("--limit", type=int, default=30, help="Max posts in export")
    export_parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Override LOOKBACK_DAYS for export",
    )
    export_parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("outputs"),
        help="Output directory for generated files",
    )

    return parser


async def _run_scrape_once(lookback_days: int | None) -> None:
    settings = load_settings()
    ensure_db(settings.db_path)
    scraper = TelegramChannelScraper(settings)
    stats = await scraper.scrape_lookback(lookback_days=lookback_days)
    logging.getLogger(__name__).info(
        "Done. channels_ok=%s channels_failed=%s posts_processed=%s",
        stats.channels_ok,
        stats.channels_failed,
        stats.posts_processed,
    )


def _print_top_posts(
    limit: int,
    lookback_days: int | None,
    show_links: bool,
) -> None:
    settings = load_settings()
    ensure_db(settings.db_path)
    window = lookback_days or settings.lookback_days
    top_posts = get_top_posts(
        settings.db_path,
        lookback_days=window,
        limit=limit,
        require_external_links=True,
        research_only=False,
        require_github=True,
    )
    if not top_posts:
        print("No data found for the selected window.")
        return

    for idx, post in enumerate(top_posts, start=1):
        print(
            f"{idx}. [{post.channel_title}] post={post.message_id} "
            f"reactions={post.latest_reactions} growth={post.reactions_growth} "
            f"views={post.latest_views or 0} forwards={post.latest_forwards or 0}"
        )
        print(f"   url={post.post_url or 'N/A'}")
        if post.article_links:
            print(f"   article_links={len(post.article_links)}")
        if post.research_links:
            print(f"   research_links={len(post.research_links)}")
        if post.github_links:
            print(f"   github_links={len(post.github_links)}")
        if show_links:
            for url in post.research_links:
                print(f"   research: {url}")
            for url in post.article_links:
                print(f"   article: {url}")
            for url in post.github_links:
                print(f"   github: {url}")


def _export_weekly(
    limit: int,
    lookback_days: int | None,
    out_dir: Path,
) -> None:
    settings = load_settings()
    ensure_db(settings.db_path)
    window = lookback_days or settings.lookback_days
    top_posts = get_top_posts(
        settings.db_path,
        lookback_days=window,
        limit=limit,
        require_external_links=True,
        research_only=False,
        require_github=True,
    )
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = out_dir / f"top_posts_{timestamp}.json"
    md_path = out_dir / f"top_posts_{timestamp}.md"

    export_top_posts_json(top_posts, json_path)
    export_top_posts_markdown(top_posts, md_path)

    print(f"Exported JSON: {json_path}")
    print(f"Exported Markdown: {md_path}")


async def _run_daily_scheduler() -> None:
    settings = load_settings()
    ensure_db(settings.db_path)
    scraper = TelegramChannelScraper(settings)
    log = logging.getLogger(__name__)

    async def job() -> None:
        log.info("Daily scrape job started")
        stats = await scraper.scrape_lookback(lookback_days=settings.lookback_days)
        log.info(
            "Daily scrape job finished. channels_ok=%s channels_failed=%s posts_processed=%s",
            stats.channels_ok,
            stats.channels_failed,
            stats.posts_processed,
        )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        lambda: asyncio.create_task(job()),
        CronTrigger(hour=settings.schedule_hour, minute=settings.schedule_minute),
        id="daily_scrape",
        replace_existing=True,
    )
    scheduler.start()
    log.info(
        "Scheduler started. Next daily run at %02d:%02d (local time).",
        settings.schedule_hour,
        settings.schedule_minute,
    )

    await job()
    await asyncio.Event().wait()


async def _run_bot() -> None:
    from tg_ml_scraper.bot_app import TelegramScraperBot

    settings = load_settings()
    ensure_db(settings.db_path)
    bot_app = TelegramScraperBot(settings)
    await bot_app.run()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    _configure_stdout()
    _setup_logging(args.verbose)

    if args.command == "scrape-once":
        asyncio.run(_run_scrape_once(args.lookback_days))
        return

    if args.command == "run-daily":
        asyncio.run(_run_daily_scheduler())
        return

    if args.command == "run-bot":
        asyncio.run(_run_bot())
        return

    if args.command == "report-top":
        _print_top_posts(
            args.limit,
            args.lookback_days,
            args.show_links,
        )
        return

    if args.command == "export-weekly":
        _export_weekly(
            args.limit,
            args.lookback_days,
            args.out_dir,
        )
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
