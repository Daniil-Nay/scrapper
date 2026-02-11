from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tg_ml_scraper.db import open_db


@dataclass(frozen=True)
class TopPost:
    channel_title: str
    channel_username: str | None
    message_id: int
    post_url: str | None
    post_datetime: str
    message_text: str | None
    latest_reactions: int
    reactions_growth: int
    latest_views: int | None
    latest_forwards: int | None
    github_links: list[str]
    research_links: list[str]
    article_links: list[str]


@dataclass(frozen=True)
class PostWithLinks:
    channel_title: str
    channel_username: str | None
    message_id: int
    post_url: str | None
    post_datetime: str
    message_text: str | None
    latest_reactions: int
    latest_views: int | None
    latest_forwards: int | None
    github_links: list[str]
    research_links: list[str]
    article_links: list[str]


def _window_start(lookback_days: int) -> date:
    return date.today() - timedelta(days=max(1, lookback_days) - 1)


def _split_links(link_rows: list) -> tuple[list[str], list[str], list[str]]:
    github_links: list[str] = []
    research_links: list[str] = []
    article_links: list[str] = []
    for row in link_rows:
        link_type = row["link_type"]
        if link_type == "github":
            github_links.append(row["url"])
        elif link_type == "research":
            research_links.append(row["url"])
        elif link_type == "article":
            article_links.append(row["url"])
    return github_links, research_links, article_links


def _passes_link_filters(
    *,
    github_links: list[str],
    research_links: list[str],
    article_links: list[str],
    require_external_links: bool,
    research_only: bool,
    require_github: bool,
) -> bool:
    if require_github and not github_links:
        return False
    has_external = bool(github_links or research_links or article_links)
    if require_external_links and not has_external:
        return False
    if research_only and not research_links:
        return False
    return True


def get_top_posts(
    db_path: Path,
    *,
    lookback_days: int,
    limit: int = 20,
    require_external_links: bool = True,
    research_only: bool = False,
    require_github: bool = False,
) -> list[TopPost]:
    start = _window_start(lookback_days).isoformat()
    candidates_limit = max(limit * 10, 200)

    with open_db(db_path) as conn:
        rows = conn.execute(
            """
            WITH window AS (
                SELECT s.*
                FROM snapshots s
                WHERE s.snapshot_date >= ?
            ),
            latest_by_post AS (
                SELECT post_id, MAX(snapshot_date) AS latest_date
                FROM window
                GROUP BY post_id
            ),
            oldest_by_post AS (
                SELECT post_id, MIN(snapshot_date) AS oldest_date
                FROM window
                GROUP BY post_id
            ),
            metrics AS (
                SELECT
                    l.post_id,
                    wl.total_reactions AS latest_reactions,
                    wo.total_reactions AS oldest_reactions,
                    wl.views AS latest_views,
                    wl.forwards AS latest_forwards
                FROM latest_by_post l
                JOIN window wl
                    ON wl.post_id = l.post_id
                   AND wl.snapshot_date = l.latest_date
                JOIN oldest_by_post o
                    ON o.post_id = l.post_id
                JOIN window wo
                    ON wo.post_id = o.post_id
                   AND wo.snapshot_date = o.oldest_date
            )
            SELECT
                p.id,
                p.channel_title,
                p.channel_username,
                p.message_id,
                p.post_url,
                p.post_datetime,
                p.message_text,
                m.latest_reactions,
                (m.latest_reactions - m.oldest_reactions) AS reactions_growth,
                m.latest_views,
                m.latest_forwards
            FROM metrics m
            JOIN posts p ON p.id = m.post_id
            ORDER BY m.latest_reactions DESC, reactions_growth DESC, p.post_datetime DESC
            LIMIT ?
            """,
            (start, candidates_limit),
        ).fetchall()

        result: list[TopPost] = []
        for row in rows:
            link_rows = conn.execute(
                "SELECT url, link_type FROM links WHERE post_id = ? ORDER BY id ASC",
                (int(row["id"]),),
            ).fetchall()
            github_links, research_links, article_links = _split_links(link_rows)
            if not _passes_link_filters(
                github_links=github_links,
                research_links=research_links,
                article_links=article_links,
                require_external_links=require_external_links,
                research_only=research_only,
                require_github=require_github,
            ):
                continue

            result.append(
                TopPost(
                    channel_title=row["channel_title"],
                    channel_username=row["channel_username"],
                    message_id=int(row["message_id"]),
                    post_url=row["post_url"],
                    post_datetime=row["post_datetime"],
                    message_text=row["message_text"],
                    latest_reactions=int(row["latest_reactions"]),
                    reactions_growth=int(row["reactions_growth"]),
                    latest_views=row["latest_views"],
                    latest_forwards=row["latest_forwards"],
                    github_links=github_links,
                    research_links=research_links,
                    article_links=article_links,
                )
            )
            if len(result) >= limit:
                break

        return result


def _post_to_dict(post: TopPost) -> dict[str, Any]:
    return {
        "channel_title": post.channel_title,
        "channel_username": post.channel_username,
        "message_id": post.message_id,
        "post_url": post.post_url,
        "post_datetime": post.post_datetime,
        "message_text": post.message_text,
        "latest_reactions": post.latest_reactions,
        "reactions_growth": post.reactions_growth,
        "latest_views": post.latest_views,
        "latest_forwards": post.latest_forwards,
        "github_links": post.github_links,
        "research_links": post.research_links,
        "article_links": post.article_links,
    }


def get_posts_with_links(
    db_path: Path,
    *,
    lookback_days: int,
    require_external_links: bool = True,
    research_only: bool = False,
    require_github: bool = False,
) -> list[PostWithLinks]:
    start_dt = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    with open_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                p.id,
                p.channel_title,
                p.channel_username,
                p.message_id,
                p.post_url,
                p.post_datetime,
                p.message_text,
                (
                    SELECT s.total_reactions
                    FROM snapshots s
                    WHERE s.post_id = p.id
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) AS latest_reactions,
                (
                    SELECT s.views
                    FROM snapshots s
                    WHERE s.post_id = p.id
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) AS latest_views,
                (
                    SELECT s.forwards
                    FROM snapshots s
                    WHERE s.post_id = p.id
                    ORDER BY s.snapshot_date DESC
                    LIMIT 1
                ) AS latest_forwards
            FROM posts p
            WHERE p.post_datetime >= ?
            ORDER BY p.post_datetime DESC
            """,
            (start_dt.isoformat(),),
        ).fetchall()

        result: list[PostWithLinks] = []
        for row in rows:
            link_rows = conn.execute(
                "SELECT url, link_type FROM links WHERE post_id = ? ORDER BY id ASC",
                (int(row["id"]),),
            ).fetchall()
            github_links, research_links, article_links = _split_links(link_rows)
            if not _passes_link_filters(
                github_links=github_links,
                research_links=research_links,
                article_links=article_links,
                require_external_links=require_external_links,
                research_only=research_only,
                require_github=require_github,
            ):
                continue
            result.append(
                PostWithLinks(
                    channel_title=row["channel_title"],
                    channel_username=row["channel_username"],
                    message_id=int(row["message_id"]),
                    post_url=row["post_url"],
                    post_datetime=row["post_datetime"],
                    message_text=row["message_text"],
                    latest_reactions=int(row["latest_reactions"] or 0),
                    latest_views=row["latest_views"],
                    latest_forwards=row["latest_forwards"],
                    github_links=github_links,
                    research_links=research_links,
                    article_links=article_links,
                )
            )
        return result


def _post_links_to_dict(post: PostWithLinks) -> dict[str, Any]:
    return {
        "channel_title": post.channel_title,
        "channel_username": post.channel_username,
        "message_id": post.message_id,
        "post_url": post.post_url,
        "post_datetime": post.post_datetime,
        "message_text": post.message_text,
        "latest_reactions": post.latest_reactions,
        "latest_views": post.latest_views,
        "latest_forwards": post.latest_forwards,
        "github_links": post.github_links,
        "research_links": post.research_links,
        "article_links": post.article_links,
    }


def export_top_posts_json(top_posts: list[TopPost], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [_post_to_dict(p) for p in top_posts]
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def export_top_posts_markdown(top_posts: list[TopPost], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Weekly Top ML/DS Posts")
    lines.append("")
    for idx, post in enumerate(top_posts, start=1):
        lines.append(f"## {idx}. {post.channel_title} / post {post.message_id}")
        lines.append(f"- Reactions: {post.latest_reactions} (growth: {post.reactions_growth})")
        lines.append(f"- Views: {post.latest_views or 0}")
        lines.append(f"- Forwards: {post.latest_forwards or 0}")
        lines.append(f"- Date: {post.post_datetime}")
        lines.append(f"- URL: {post.post_url or 'N/A'}")
        if post.article_links:
            lines.append("- Article links:")
            for url in post.article_links:
                lines.append(f"  - {url}")
        if post.research_links:
            lines.append("- Research links:")
            for url in post.research_links:
                lines.append(f"  - {url}")
        if post.github_links:
            lines.append("- GitHub links:")
            for url in post.github_links:
                lines.append(f"  - {url}")
        if post.message_text:
            snippet = post.message_text.replace("\n", " ").strip()
            if len(snippet) > 280:
                snippet = snippet[:277] + "..."
            lines.append(f"- Text: {snippet}")
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def export_posts_with_links_json(posts: list[PostWithLinks], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [_post_links_to_dict(p) for p in posts]
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
