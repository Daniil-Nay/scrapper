from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urlparse

from telethon.tl.custom.message import Message
from telethon.tl.types import MessageEntityTextUrl


URL_REGEX = re.compile(r"(https?://[^\s)\]>\"']+)")
RESEARCH_HOST_SUFFIXES = (
    "arxiv.org",
    "openreview.net",
    "aclanthology.org",
    "proceedings.mlr.press",
    "jmlr.org",
    "paperswithcode.com",
    "ieeexplore.ieee.org",
    "link.springer.com",
    "sciencedirect.com",
    "nature.com",
    "science.org",
    "doi.org",
)
TELEGRAM_HOST_SUFFIXES = (
    "t.me",
    "telegram.me",
    "telegram.dog",
    "telegram.org",
)


def extract_urls(message: Message) -> list[str]:
    urls: set[str] = set()
    text = message.message or ""
    for match in URL_REGEX.findall(text):
        urls.add(match.rstrip(".,;:!?"))

    entities = message.entities or []
    for entity in entities:
        if isinstance(entity, MessageEntityTextUrl) and entity.url:
            urls.add(entity.url.strip())

    return sorted(urls)


def classify_link(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if "github.com" in host:
        return "github"
    for suffix in TELEGRAM_HOST_SUFFIXES:
        if host == suffix or host.endswith("." + suffix):
            return "telegram"
    for suffix in RESEARCH_HOST_SUFFIXES:
        if host == suffix or host.endswith("." + suffix):
            return "research"
    if host:
        return "article"
    return "other"


def links_with_types(urls: Iterable[str]) -> list[tuple[str, str]]:
    return [(url, classify_link(url)) for url in urls]


def extract_reactions(message: Message) -> dict[str, int]:
    results: dict[str, int] = {}
    reactions = getattr(message, "reactions", None)
    if not reactions or not getattr(reactions, "results", None):
        return results

    for item in reactions.results:
        key = "unknown"
        reaction_obj = getattr(item, "reaction", None)
        if reaction_obj is not None:
            emoticon = getattr(reaction_obj, "emoticon", None)
            document_id = getattr(reaction_obj, "document_id", None)
            key = emoticon or (f"custom_{document_id}" if document_id else str(reaction_obj))
        results[key] = int(getattr(item, "count", 0))
    return results


def total_reactions(reactions: dict[str, int]) -> int:
    return sum(reactions.values())
