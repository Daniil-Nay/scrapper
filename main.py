from __future__ import annotations

import sys

from tg_ml_scraper.main import main as cli_main


if __name__ == "__main__":
    sys.argv = ["main.py", "run-bot"]
    cli_main()

