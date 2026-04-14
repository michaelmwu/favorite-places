"""Browser-backed Google Maps saved-list scraper."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from google_saved_lists.models import SavedList
from google_saved_lists.parser import JSONValue, parse_saved_list_artifacts

_CONSENT_URL_MARKERS = ("consent.google", "consent.youtube")
_CONSENT_TEXT_MARKERS = (
    "before you continue to google",
    "prima di continuare su google",
    "avant de continuer sur google",
    "bevor sie zu google weitergehen",
    "antes de continuar en google",
    "voordat je doorgaat naar google",
    "g.co/privacytools",
)
_REJECT_BUTTON_LABELS = (
    "Reject all",
    "Rifiuta tutto",
    "Tout refuser",
    "Alle ablehnen",
    "Rechazar todo",
    "Tudo recusar",
    "Alles afwijzen",
    "Odrzuć wszystko",
    "Avvisa allt",
    "Afvis alle",
    "Hylää kaikki",
)
_MORE_OPTIONS_BUTTON_LABELS = (
    "More options",
    "Altre opzioni",
    "Plus d'options",
    "Weitere Optionen",
    "Más opciones",
    "Mais opções",
    "Meer opties",
    "Więcej opcji",
    "Fler alternativ",
    "Flere valgmuligheder",
    "Lisää vaihtoehtoja",
)


@dataclass(slots=True)
class BrowserArtifacts:
    """Artifacts collected from a browser session."""

    resolved_url: str | None
    runtime_state: JSONValue | None
    script_texts: list[str]
    html: str


class ScrapeError(RuntimeError):
    """Raised when browser automation fails."""


def scrape_saved_list(
    list_url: str,
    *,
    headless: bool = True,
    timeout_ms: int = 30_000,
    settle_time_ms: int = 3_000,
) -> SavedList:
    """Scrape and parse a Google Maps saved list."""
    artifacts = collect_browser_artifacts(
        list_url,
        headless=headless,
        timeout_ms=timeout_ms,
        settle_time_ms=settle_time_ms,
    )
    return parse_saved_list_artifacts(
        list_url,
        resolved_url=artifacts.resolved_url,
        runtime_state=artifacts.runtime_state,
        script_texts=artifacts.script_texts,
        html=artifacts.html,
    )


def collect_browser_artifacts(
    list_url: str,
    *,
    headless: bool,
    timeout_ms: int,
    settle_time_ms: int,
) -> BrowserArtifacts:
    """Load a page in CloakBrowser and collect runtime artifacts."""
    try:
        from cloakbrowser import launch  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dependency error path
        raise ScrapeError("CloakBrowser is not installed. Run `uv sync`.") from exc

    browser = launch(headless=headless, humanize=True)
    try:
        page = browser.new_page()
        page.goto(list_url, wait_until="domcontentloaded", timeout=timeout_ms)
        _handle_google_consent(page, timeout_ms=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except Exception:
            pass
        _handle_google_consent(page, timeout_ms=timeout_ms)
        page.wait_for_timeout(settle_time_ms)

        resolved_url = _read_resolved_url(page)
        runtime_state = _read_runtime_state(page, timeout_ms=timeout_ms)
        script_texts = _read_script_texts(page)
        html = page.content()
    except Exception as exc:  # pragma: no cover - browser error path
        raise ScrapeError(f"Failed to collect browser artifacts: {exc}") from exc
    finally:
        browser.close()

    return BrowserArtifacts(
        resolved_url=resolved_url,
        runtime_state=runtime_state,
        script_texts=script_texts,
        html=html,
    )


def _read_resolved_url(page: Any) -> str | None:
    value = getattr(page, "url", None)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized


def _read_runtime_state(page: Any, *, timeout_ms: int) -> JSONValue | None:
    attempts = max(1, timeout_ms // 1_000)
    for _ in range(attempts):
        runtime_state = page.evaluate(
            "() => globalThis.APP_INITIALIZATION_STATE ?? window.APP_INITIALIZATION_STATE ?? null"
        )
        if isinstance(runtime_state, (list, dict)):
            return runtime_state
        page.wait_for_timeout(1_000)
    return None


def _read_script_texts(page: Any) -> list[str]:
    script_texts = page.evaluate(
        "() => Array.from(document.scripts, (script) => script.textContent || '')"
    )
    if not isinstance(script_texts, list):
        return []
    return [text for text in script_texts if isinstance(text, str)]


def _handle_google_consent(page: Any, *, timeout_ms: int) -> None:
    for _ in range(2):
        if not _has_google_consent_screen(page):
            return
        if _click_button_in_contexts(page, _REJECT_BUTTON_LABELS):
            _settle_after_consent(page, timeout_ms=timeout_ms)
            continue
        if _click_button_in_contexts(page, _MORE_OPTIONS_BUTTON_LABELS):
            page.wait_for_timeout(500)
            if _click_button_in_contexts(page, _REJECT_BUTTON_LABELS):
                _settle_after_consent(page, timeout_ms=timeout_ms)
                continue
        break

    if not _has_google_consent_screen(page):
        return

    diagnostics = _capture_consent_diagnostics(page)
    details = ", ".join(str(path) for path in diagnostics)
    raise ScrapeError(
        f"Detected a Google consent screen but could not reject cookies automatically. "
        f"Saved diagnostics: {details}"
    )


def _settle_after_consent(page: Any, *, timeout_ms: int) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass
    page.wait_for_timeout(1_000)


def _has_google_consent_screen(page: Any) -> bool:
    url = str(getattr(page, "url", "")).lower()
    if any(marker in url for marker in _CONSENT_URL_MARKERS):
        return True

    for context in _iter_contexts(page):
        body_text = _read_body_text(context).lower()
        if any(marker in body_text for marker in _CONSENT_TEXT_MARKERS):
            return True
    return False


def _click_button_in_contexts(page: Any, labels: tuple[str, ...]) -> bool:
    pattern = _button_label_pattern(labels)
    for context in _iter_contexts(page):
        if _click_button(context, pattern=pattern, labels=labels):
            return True
    return False


def _click_button(context: Any, *, pattern: re.Pattern[str], labels: tuple[str, ...]) -> bool:
    try:
        context.get_by_role("button", name=pattern).first.click(timeout=1_500)
    except Exception:
        return _click_button_with_dom(context, labels)
    return True


def _button_label_pattern(labels: tuple[str, ...]) -> re.Pattern[str]:
    escaped_labels = [re.escape(label) for label in labels]
    return re.compile(rf"^\s*(?:{'|'.join(escaped_labels)})\s*$", re.IGNORECASE)


def _iter_contexts(page: Any) -> list[Any]:
    contexts = [page]
    for frame in getattr(page, "frames", []):
        if frame is page:
            continue
        contexts.append(frame)
    return contexts


def _read_body_text(context: Any) -> str:
    try:
        value = context.evaluate("() => document.body?.innerText ?? ''")
    except Exception:
        return ""
    if not isinstance(value, str):
        return ""
    return value


def _click_button_with_dom(context: Any, labels: tuple[str, ...]) -> bool:
    script = """
    (labels) => {
      const normalize = (value) => value.trim().replace(/\\s+/g, " ").toLowerCase();
      const expected = new Set(labels.map(normalize));
      const selector = [
        "button",
        '[role="button"]',
        'input[type="button"]',
        'input[type="submit"]'
      ].join(", ");
      const candidates = Array.from(document.querySelectorAll(selector));

      for (const element of candidates) {
        const text = normalize(element.innerText || element.textContent || element.value || "");
        if (!expected.has(text)) {
          continue;
        }
        element.click();
        return true;
      }
      return false;
    }
    """
    try:
        clicked = context.evaluate(script, list(labels))
    except Exception:
        return False
    return clicked is True


def _capture_consent_diagnostics(page: Any) -> list[Path]:
    diagnostics_dir = Path(".context/diagnostics")
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    saved_paths: list[Path] = []
    html_path = diagnostics_dir / f"google-consent-{timestamp}.html"
    text_path = diagnostics_dir / f"google-consent-{timestamp}.txt"
    screenshot_path = diagnostics_dir / f"google-consent-{timestamp}.png"

    try:
        html_path.write_text(page.content(), encoding="utf-8")
        saved_paths.append(html_path)
    except Exception:
        pass

    try:
        text_path.write_text(_read_body_text(page), encoding="utf-8")
        saved_paths.append(text_path)
    except Exception:
        pass

    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
        saved_paths.append(screenshot_path)
    except Exception:
        pass

    return saved_paths
