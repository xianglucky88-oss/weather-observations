from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class SourceError(RuntimeError):
    """Base class for source failures that stop the current run."""


class SourceRequestError(SourceError):
    """The transport failed; nothing was consumed."""


class SourceResponseError(SourceError):
    """The response was retrieved but is not a valid incremental page."""


class SourceProtocolError(SourceError):
    """The provider violated pagination rules (cursor cycle, page limit)."""


@dataclass(frozen=True)
class Page:
    """A parsed source page.  ``body`` stays untouched for evidence storage."""

    body: bytes
    raw_sha256: str
    records: list[Any]
    cursor_in: str | None
    cursor_out: str | None
    next_url: str | None


@dataclass
class PageChain:
    """Pagination hints extracted from one JSON object."""

    records: list[Any]
    next_cursor: str | None
    next_url: str | None


_RECORD_KEYS = ("records", "data", "items", "results")
_CONTAINER_KEYS = ("pagination", "page", "meta", "links")
_NEXT_CURSOR_KEYS = ("next_cursor", "nextCursor", "next_page_cursor",
                     "nextPageToken", "next_page_token")
_CURSOR_KEYS = ("cursor", "current_cursor", "page_cursor")
_NEXT_URL_KEYS = ("next_url", "nextUrl", "next_page_url", "next_link")
_NEXT_OBJECT_KEYS = ("next",)
_QUERY_RE = re.compile(r"\?[^\s'\"<>]*")
_USERINFO_RE = re.compile(r"(?<=//)[^/\s:@]+:[^/\s@]+@")
_SENSITIVE_QUERY = re.compile(
    r"(token|secret|signature|sig|api[-_]?key|apikey|password|passwd|credential|auth)",
    re.IGNORECASE,
)


def redact_url(url: str | None) -> str | None:
    """Drop userinfo, query and fragment so history never stores credentials."""
    if url is None:
        return None
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return "<invalid-url>"
    host = parts.hostname or ""
    try:
        port = parts.port
    except ValueError:
        port = None
    if port is not None:
        host = f"{host}:{port}"
    path = parts.path or ""
    if parts.scheme and not path and not host:
        return parts.scheme
    return urllib.parse.urlunsplit((parts.scheme, host, path, "", ""))


def safe_to_persist(url: str) -> bool:
    """True when a next-page URL can be stored without keeping credentials.

    Query strings are allowed for ordinary filters, but links carrying
    userinfo or credential-looking parameters (typical signed URLs) are not
    persisted; the checkpoint then rebuilds the request from its cursor.
    """
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return False
    if parts.scheme not in ("http", "https", "fixture"):
        return False
    if parts.username or parts.password:
        return False
    for key, _value in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
        if _SENSITIVE_QUERY.search(key):
            return False
    return True


def redact_message(message: str) -> str:
    """Strip URL query strings and userinfo from a diagnostic string."""
    return _USERINFO_RE.sub("<redacted>@", _QUERY_RE.sub("?<redacted>", message))


def _first_string(mapping: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _nested_candidates(value: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [value]
    for key in _CONTAINER_KEYS:
        nested = value.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)
    return candidates


def select_records(value: Any, records_key: str | None) -> list[Any]:
    selected = value
    if records_key:
        for part in records_key.split("."):
            if not isinstance(selected, dict) or part not in selected:
                raise SourceResponseError(f"records key not found: {records_key}")
            selected = selected[part]
    elif isinstance(selected, dict):
        for key in _RECORD_KEYS:
            candidate = selected.get(key)
            if isinstance(candidate, list):
                selected = candidate
                break
    if not isinstance(selected, list):
        raise SourceResponseError("source response must contain a JSON list of records")
    return selected


def parse_page(body: bytes, *, records_key: str | None = None) -> PageChain:
    """Parse and validate one page.  Raises ``SourceResponseError`` on garbage."""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourceResponseError("source response is not valid UTF-8") from exc
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        # The decoder message can contain a snippet of the payload; keep the
        # diagnostic free-form and credential-free instead of echoing it.
        raise SourceResponseError("source response is not valid JSON") from exc

    if isinstance(value, list):
        return PageChain(records=value, next_cursor=None, next_url=None)
    if not isinstance(value, dict):
        raise SourceResponseError("source response must be a JSON object or list")

    records = select_records(value, records_key)
    next_cursor = next_url = None
    for candidate in _nested_candidates(value):
        if next_cursor is None:
            next_cursor = _first_string(candidate, _NEXT_CURSOR_KEYS)
        if next_url is None:
            next_url = _first_string(candidate, _NEXT_URL_KEYS)

    for candidate in _nested_candidates(value):
        for key in _NEXT_OBJECT_KEYS:
            nxt = candidate.get(key)
            if isinstance(nxt, dict):
                if next_cursor is None:
                    next_cursor = _first_string(nxt, _CURSOR_KEYS + _NEXT_CURSOR_KEYS)
                if next_url is None:
                    next_url = _first_string(nxt, _NEXT_URL_KEYS + ("href", "url"))
            elif isinstance(nxt, str) and nxt:
                if "://" in nxt or nxt.startswith("/"):
                    if next_url is None:
                        next_url = nxt
                elif next_cursor is None:
                    next_cursor = nxt

    return PageChain(records=records, next_cursor=next_cursor, next_url=next_url)


def _same_origin(url: str, origin: str) -> bool:
    try:
        target = urllib.parse.urlsplit(url)
        base = urllib.parse.urlsplit(origin)
    except ValueError:
        return False
    return (target.scheme, target.hostname, target.port) == (
        base.scheme, base.hostname, base.port)


def auth_headers(
    endpoint: str,
    *,
    url: str | None = None,
    bearer_token_env: str = "DATA_SOURCE_BEARER_TOKEN",
    api_key_env: str = "DATA_SOURCE_API_KEY",
) -> dict[str, str]:
    """Build request headers, never leaking credentials cross-origin."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "data-ingestion-adapter/0.1",
    }
    if url is not None and not _same_origin(url, endpoint):
        return headers
    token = os.getenv(bearer_token_env)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    api_key = os.getenv(api_key_env)
    if api_key:
        headers["X-API-Key"] = api_key
    return headers


class FixtureSource:
    """Legacy single-shot fixture reader (deterministic, no network)."""

    def read(self, path: str) -> list[Any]:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, list):
            raise ValueError("fixture must contain a JSON list")
        return value


class HttpJsonSource:
    """Legacy one-shot HTTP JSON reader retained for backwards compatibility."""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        raw_dir: str | None = None,
        records_key: str | None = None,
        bearer_token_env: str = "DATA_SOURCE_BEARER_TOKEN",
        api_key_env: str = "DATA_SOURCE_API_KEY",
    ) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must use http:// or https://")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.url = url
        self.timeout = timeout
        self.raw_dir = Path(raw_dir) if raw_dir else None
        self.records_key = records_key
        self.bearer_token_env = bearer_token_env
        self.api_key_env = api_key_env

    def _headers(self) -> dict[str, str]:
        return auth_headers(
            self.url,
            bearer_token_env=self.bearer_token_env,
            api_key_env=self.api_key_env,
        )

    def _save_raw(self, body: bytes) -> None:
        if self.raw_dir is None:
            return
        from .rawstore import RawResponseStore

        RawResponseStore(self.raw_dir).save(body)

    def read(self, _path: str | None = None) -> list[Any]:
        request = urllib.request.Request(self.url, headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"source returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"source request failed: {exc.reason}") from exc
        self._save_raw(body)
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("source response is not valid UTF-8 JSON") from exc
        return select_records(value, self.records_key)


class IncrementalHttpSource:
    """Transport + strict page parser for a cursor-based JSON endpoint.

    The client never decides whether a page counts as consumed; it only fetches
    bytes and parses them.  Raw bytes are persisted by the runner before any
    parsing or database work, which keeps evidence ordering explicit.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        records_key: str | None = None,
        cursor_param: str = "cursor",
        bearer_token_env: str = "DATA_SOURCE_BEARER_TOKEN",
        api_key_env: str = "DATA_SOURCE_API_KEY",
        opener: Any | None = None,
    ) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must use http:// or https://")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.url = url
        self.timeout = timeout
        self.records_key = records_key
        self.cursor_param = cursor_param
        self.bearer_token_env = bearer_token_env
        self.api_key_env = api_key_env
        self._opener = opener or urllib.request.urlopen

    def _headers(self) -> dict[str, str]:
        return self._auth_headers(self.url)

    def _auth_headers(self, url: str | None = None) -> dict[str, str]:
        return auth_headers(
            self.url,
            url=url,
            bearer_token_env=self.bearer_token_env,
            api_key_env=self.api_key_env,
        )

    def url_for_cursor(self, cursor: str) -> str:
        parts = urllib.parse.urlsplit(self.url)
        query = [(key, value) for key, value in urllib.parse.parse_qsl(
            parts.query, keep_blank_values=True) if key != self.cursor_param]
        query.append((self.cursor_param, cursor))
        return urllib.parse.urlunsplit(
            (parts.scheme, parts.netloc, parts.path,
             urllib.parse.urlencode(query), parts.fragment))

    def fetch_raw(self, url: str) -> bytes:
        request = urllib.request.Request(url, headers=self._auth_headers(url), method="GET")
        try:
            with self._opener(request, timeout=self.timeout) as response:
                status = getattr(response, "status", 200)
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise SourceRequestError(f"source returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            reason = exc.reason if isinstance(exc.reason, str) else type(exc.reason).__name__
            raise SourceRequestError(
                f"source request failed: {redact_message(reason)}") from exc
        except OSError as exc:
            raise SourceRequestError(
                f"source request failed: {redact_message(str(exc))}") from exc
        if isinstance(status, int) and not 200 <= status < 300:
            raise SourceRequestError(f"source returned HTTP {status}")
        if not isinstance(body, (bytes, bytearray)):
            raise SourceRequestError("source response body is not bytes")
        return bytes(body)

    def parse(self, body: bytes, cursor_in: str | None) -> Page:
        chain = parse_page(body, records_key=self.records_key)
        next_url = chain.next_url
        if next_url and "://" not in next_url:
            if not next_url.startswith("/"):
                raise SourceResponseError("next page url must be absolute or root-relative")
            # Relative links are resolved against the URL that produced them.
            next_url = urllib.parse.urljoin(self.url, next_url)
        return Page(
            body=body,
            raw_sha256=hashlib.sha256(body).hexdigest(),
            records=chain.records,
            cursor_in=cursor_in,
            cursor_out=chain.next_cursor,
            next_url=next_url,
        )


class FixturePageSource:
    """Deterministic, offline page source that reuses the HTTP page parser.

    The fixture is a JSON object::

        {"pages": [
            {"url": "fixture://page-1", "records": [...],
             "next_cursor": "c2", "next_url": "fixture://page-2"},
            {"url": "fixture://page-2", "records": [...]}
        ]}

    Addresses are opaque local labels (never real hosts), so a checkpoint
    written against a fixture can be replayed exactly by the same fixture.
    """

    def __init__(self, path: str | Path, *, records_key: str | None = None) -> None:
        self.records_key = records_key
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, dict) or not isinstance(value.get("pages"), list):
            raise ValueError("paged fixture must be an object with a 'pages' list")
        self.pages: dict[str, dict[str, Any]] = {}
        for index, page in enumerate(value["pages"]):
            if not isinstance(page, dict):
                raise ValueError("each fixture page must be an object")
            url = page.get("url", f"fixture://page-{index + 1}")
            if not isinstance(url, str) or not url:
                raise ValueError("fixture page url must be a non-empty string")
            if url in self.pages:
                raise ValueError(f"duplicate fixture page url: {url}")
            self.pages[url] = page
        self.url = str(value.get("start_url") or next(iter(self.pages), "fixture://page-1"))

    def url_for_cursor(self, cursor: str) -> str:
        # Prefer the page whose *own* cursor is this value; a page that merely
        # advertises it as its successor must not shadow the real match.
        for url, page in self.pages.items():
            if page.get("cursor") == cursor:
                return url
        for url, page in self.pages.items():
            if page.get("next_cursor") == cursor:
                return url
        raise SourceResponseError("fixture has no page for the requested cursor")

    def fetch_raw(self, url: str) -> bytes:
        try:
            page = self.pages[url]
        except KeyError as exc:
            raise SourceRequestError(f"fixture page not found: {redact_url(url)}") from exc
        # Re-encode through the normal page shape so the production parser is
        # exercised even in offline tests.
        payload = {
            "records": page.get("records", []),
            "next_cursor": page.get("next_cursor"),
            "next_url": page.get("next_url"),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")

    def parse(self, body: bytes, cursor_in: str | None) -> Page:
        chain = parse_page(body, records_key=self.records_key)
        return Page(body=body, raw_sha256=hashlib.sha256(body).hexdigest(),
                    records=chain.records, cursor_in=cursor_in,
                    cursor_out=chain.next_cursor, next_url=chain.next_url)
