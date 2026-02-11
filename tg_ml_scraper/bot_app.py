from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from tg_ml_scraper.config import Settings
from tg_ml_scraper.db import ensure_db
from tg_ml_scraper.reporting import get_top_posts
from tg_ml_scraper.service import TelegramChannelScraper


logger = logging.getLogger(__name__)


def _split_text(text: str, *, limit: int = 3900) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in text.splitlines():
        extra = len(line) + 1
        if current and current_len + extra > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_len = extra
        else:
            current.append(line)
            current_len += extra

    if current:
        chunks.append("\n".join(current))
    return chunks


def _parse_int_args(command: CommandObject | None) -> list[int]:
    if not command or not command.args:
        return []

    values: list[int] = []
    for token in command.args.split():
        values.append(int(token))
    return values


class TelegramScraperBot:
    def __init__(self, settings: Settings) -> None:
        if not settings.bot_token:
            raise ValueError("BOT_TOKEN is required for run-bot mode.")

        self.settings = settings
        self.router = Router()
        self.scraper = TelegramChannelScraper(settings)
        self.scrape_lock = asyncio.Lock()
        ensure_db(self.settings.db_path)
        self._register_handlers()

    def _register_handlers(self) -> None:
        @self.router.message(Command("start", "help"))
        async def help_handler(message: Message) -> None:
            await message.answer(
                "команды доступные:\n"
                "/scrape [days] - запустить сбор (только с гитх)\n"
                "/top [limit] [days] - топ постов"
            )

        @self.router.message(Command("scrape"))
        async def scrape_handler(message: Message, command: CommandObject) -> None:
            try:
                args = _parse_int_args(command)
                lookback_days = args[0] if args else self.settings.lookback_days
            except ValueError:
                await message.answer("Формат команды: /scrape [days]")
                return

            if self.scrape_lock.locked():
                await message.answer("сбор азпущен")
                return

            async with self.scrape_lock:
                await message.answer(f"Запустил сбор. Окно: {lookback_days} дн.")
                stats = await self.scraper.scrape_lookback(lookback_days=lookback_days)
                await message.answer(
                    "Сбор завершен\n"
                    f"Успешно обработано каналов: {stats.channels_ok}\n"
                    f"Каналов с ошибками: {stats.channels_failed}\n"
                    f"Обработано постов: {stats.posts_processed}"
                )

        @self.router.message(Command("top"))
        async def top_handler(message: Message, command: CommandObject) -> None:
            try:
                args = _parse_int_args(command)
                limit = args[0] if len(args) >= 1 else 10
                lookback_days = args[1] if len(args) >= 2 else self.settings.lookback_days
            except ValueError:
                await message.answer("фрмат команды: /top [limit] [days]")
                return

            top_posts = get_top_posts(
                self.settings.db_path,
                lookback_days=lookback_days,
                limit=limit,
                require_github=True,
            )
            if not top_posts:
                await message.answer("нет данных - запустите  /scrape")
                return

            lines: list[str] = []
            lines.append(f"Топ постов за {lookback_days} дн.:")
            lines.append("")

            for idx, post in enumerate(top_posts, start=1):
                lines.append(
                    f"{idx}. {post.channel_title} #{post.message_id} | "
                    f"реакции={post.latest_reactions} прирост={post.reactions_growth}"
                )
                lines.append(f"дата: {post.post_datetime}")
                if post.post_url:
                    lines.append(f"пост: {post.post_url}")
                for url in post.research_links:
                    lines.append(f"исследование: {url}")
                for url in post.article_links:
                    lines.append(f"статья: {url}")
                for url in post.github_links:
                    lines.append(f"github: {url}")
                lines.append("")

            text = "\n".join(lines).strip()
            for chunk in _split_text(text):
                await message.answer(chunk)

    async def run(self) -> None:
        bot = Bot(token=self.settings.bot_token)
        dispatcher = Dispatcher()
        dispatcher.include_router(self.router)

        logger.info("Bot polling started")
        try:
            await dispatcher.start_polling(bot)
        finally:
            await bot.session.close()
