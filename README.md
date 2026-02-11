# TG Scraper

- собирает посты за окно `LOOKBACK_DAYS`
- извлекает ссылки из постов и классифицирует их как `research` / `article` / `github`
- в отчеты и выгрузки включает только посты, где есть `github` ссылка
- строит топ популярных постов за последнюю неделю (или другое окно)

поляя:
- `TG_API_ID`, `TG_API_HASH` - берутся в `https://my.telegram.org`
- `BOT_TOKEN` - токен бота из `@BotFather` (нужен для `run-bot`)
- `TG_CHANNELS` - список публичых каналов через запятую без `@`
- `LOOKBACK_DAYS` - окно сканирования и аналитики
- `SCHEDULE_HOUR`, `SCHEDULE_MINUTE` - время ежедневного запуска
- `DB_PATH` - путь к SQLite базе

одноразовый скрап:
```powershell
python -m tg_ml_scraper scrape-once
```

ежедневный режим (планировщик + первый запуск сразу):
```powershell
python -m tg_ml_scraper run-daily
```

режим Telegram-бота (команды в чате):
```powershell
python -m tg_ml_scraper run-bot
```

топ постов в консоль:
```powershell
python -m tg_ml_scraper report-top --limit 20
```

топ постов с выводом URL ссылок (`research/article/github`):
```powershell
python -m tg_ml_scraper report-top --limit 20 --show-links
```

экспорт недельного топа:
```powershell
python -m tg_ml_scraper export-weekly --limit 30 --out-dir outputs
```

## команды бота
- `/scrape [days]` - запускает сбор
- `/top [limit] [days]` - присылает топ постов с `github` (и сопутствующими `research/article` ссылками)
