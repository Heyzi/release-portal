from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, List, Dict

import markdown as md  # pip install markdown
import bleach  # pip install bleach

# -----------------------------
# Markdown / HTML sanitization config
# -----------------------------
MD_EXTENSIONS = ("fenced_code", "tables", "sane_lists")

BLEACH_ALLOWED_TAGS: List[str] = [
    "p", "br", "hr", "blockquote", "pre", "code", "kbd", "em", "strong", "del",
    "ul", "ol", "li", "h1", "h2", "h3", "h4", "h5", "h6", "a", "img",
    "table", "thead", "tbody", "tr", "th", "td",
]

BLEACH_ALLOWED_ATTRS: Dict[str, List[str]] = {
    "a": ["href", "title", "rel", "target"],
    "img": ["src", "alt", "title"],
    "code": ["class"],
    "pre": ["class"],
    "th": ["align"],
    "td": ["align"],
}

BLEACH_ALLOWED_PROTOCOLS = ["http", "https", "mailto"]


# -----------------------------
# Internal helpers
# -----------------------------
def _find_notes_file(vdir: Path) -> Optional[Path]:
    """
    Locate a release notes file inside a release directory.
    Priority order:
      1. release.md
      2. readme.md
    Filename comparison is case-insensitive.
    """
    candidates: List[Path] = []
    try:
        for f in vdir.iterdir():
            if not f.is_file():
                continue
            name_lower = f.name.lower()
            if name_lower in ("release.md", "readme.md"):
                candidates.append(f)
    except OSError:
        return None

    if not candidates:
        return None

    candidates.sort(key=lambda p: (0 if p.name.lower() == "release.md" else 1, p.name.lower()))
    return candidates[0]


def sanitize_html(html: str) -> str:
    """
    Sanitize HTML by removing unsafe tags/attributes and linkifying URLs.
    """
    cleaned = bleach.clean(
        html,
        tags=BLEACH_ALLOWED_TAGS,
        attributes=BLEACH_ALLOWED_ATTRS,
        protocols=BLEACH_ALLOWED_PROTOCOLS,
        strip=True,
    )
    return bleach.linkify(
        cleaned,
        callbacks=[bleach.callbacks.nofollow, bleach.callbacks.target_blank],
        skip_tags=["pre", "code"],
        parse_email=True,
    )


def render_markdown_to_safe_html(markdown_text: str) -> str:
    """
    Render Markdown to HTML5 and sanitize the resulting HTML.
    """
    raw_html = md.markdown(
        markdown_text or "",
        extensions=list(MD_EXTENSIONS),
        output_format="html5",
    )
    return sanitize_html(raw_html)


# -----------------------------
# Public API
# -----------------------------
def read_release_notes(vdir: Path) -> Optional[Tuple[str, str, str]]:
    """
    Read release notes from release.md or readme.md and return:
      (filename, raw_markdown_text, sanitized_html)

    Returns None if notes are missing or cannot be read.
    """
    notes_file = _find_notes_file(vdir)
    if not notes_file:
        return None

    try:
        markdown_text = notes_file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    html = render_markdown_to_safe_html(markdown_text)
    return notes_file.name, markdown_text, html
