from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .models import Observation
from .rawstore import RawResponseStore
from .sources import (
    Page,
    SourceError,
    SourceProtocolError,
    SourceResponseError,
    redact_message,
    redact_url,
    safe_to_persist,
)
from .storage import ObservationStore

Normalizer = Callable[[Any], Observation]

OK_STATUSES = frozenset(
    {"first_run", "incremental", "no_new_data", "no_progress", "duplicate_page"}
)


@dataclass
class SyncResult:
    """Machine-readable summary of one incremental run.

    ``status`` is one of:

    ``first_run``      no checkpoint existed; at least one page was committed;
    ``incremental``    a checkpoint existed and the run advanced it;
    ``no_new_data``    a checkpoint existed; pages were fetched but nothing new
                       arrived and the cursor did not move;
    ``no_progress``    the checkpoint is terminal and the provider offers no
                       address to poll;
    ``duplicate_page`` the provider re-served already-consumed bytes; the
                       checkpoint deliberately did not advance;
    ``cycle_detected`` the provider looped; committed pages stay committed but
                       the checkpoint cannot advance, so the run is a failure;
    ``failed``         transport/validation/write failure before commit;
    ``interrupted``    the process died mid-run; the last committed checkpoint
                       is intact and the next run resumes from it.
    """

    status: str
    run_id: str
    stream: str
    pages: int = 0
    duplicate_pages: int = 0
    processed: int = 0
    changed: int = 0
    rejected: int = 0
    committed_through: str | None = None
    end_cursor: str | None = None
    error: str | None = None
    observations: int = 0
    rejected_total: int = 0

    @property
    def ok(self) -> bool:
        return self.status in OK_STATUSES

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "run_id": self.run_id,
            "stream": self.stream,
            "pages": self.pages,
            "duplicate_pages": self.duplicate_pages,
            "processed": self.processed,
            "changed": self.changed,
            "rejected": self.rejected,
            "committed_through": self.committed_through,
            "end_cursor": self.end_cursor,
            "error": self.error,
            "observations": self.observations,
            "rejected_total": self.rejected_total,
        }

    def __getitem__(self, key: str) -> Any:
        # Keeps the CLI/application surface compatible with the old dict result.
        return self.as_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.as_dict().get(key, default)


class SyncRunner:
    """Resumable, cursor-based incremental synchronization.

    Per page the commit order is fixed and auditable:

    1. fetch the raw bytes;
    2. persist them to the content-addressed raw store and verify the digest
       (the evidence step — a failure here aborts before any DB work);
    3. parse and validate the page (the response-validity step);
    4. in one ``BEGIN IMMEDIATE`` transaction: upsert observations, insert
       newly-seen rejections, append the page ledger row, and advance the
       checkpoint;
    5. commit, then update the run summary.

    The checkpoint can therefore only point at a page whose raw evidence and
    data rows are already durable.  A crash before step 4 commits leaves the
    previous checkpoint untouched; the next run re-fetches that page.  Writes
    and rejection inserts are idempotent, so replay changes nothing but the
    page ledger, which records the replay explicitly.

    Cursor semantics: ``cursor`` in the checkpoint is the cursor that travels
    on the *next* request.  When a response supplies ``next_cursor`` the
    checkpoint advances to it.  When it does not, the page is the provider's
    current end: the checkpoint keeps the cursor that produced it and stores
    the page's own URL in ``next_url``, so the next run polls exactly that
    position.  Providers that append data later extend that page; one with
    nothing new returns identical bytes, detected as a byte-level replay that
    does not advance or double-count.

    Cycle handling: a cursor or link that repeats inside one run is classified
    as a cycle.  Records never seen before are still consumed exactly once, but
    the checkpoint is pinned (same cursor, no forward link) so a re-run replays
    identical bytes and stops instead of walking the loop again.
    """

    def __init__(
        self,
        source: Any,
        store: ObservationStore,
        normalizer: Normalizer,
        raw_store: RawResponseStore,
        *,
        stream: str,
        max_pages: int = 1000,
    ) -> None:
        if not stream:
            raise ValueError("stream is required")
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        self.source = source
        self.store = store
        self.normalizer = normalizer
        self.raw_store = raw_store
        self.stream = stream
        self.max_pages = max_pages

    # -- public ----------------------------------------------------------

    def run(self, run_id: str | None = None) -> SyncResult:
        run_id = run_id or uuid.uuid4().hex
        checkpoint = self.store.get_checkpoint(self.stream)
        run_kind = "first_run" if checkpoint is None else "incremental"
        start_cursor = checkpoint.cursor if checkpoint else None
        result = SyncResult(status=run_kind, run_id=run_id, stream=self.stream,
                            end_cursor=start_cursor)
        self.store.start_run(run_id, self.stream, run_kind, start_cursor)

        try:
            self._drain(result, run_id, checkpoint)
        except SourceError as exc:
            self._finalize(result, run_id, status="failed", error=_describe(exc))
            raise
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            self._finalize(result, run_id, status="failed", error=_describe(exc))
            raise
        except BaseException as exc:  # KeyboardInterrupt, SystemExit, ...
            self._finalize(result, run_id, status="interrupted",
                           error=_describe(exc))
            raise

        self._finalize(result, run_id, status=result.status, error=result.error)
        return result

    # -- internals -------------------------------------------------------

    def _start_url(self, checkpoint: Any) -> str | None:
        if checkpoint is None:
            return self.source.url
        if checkpoint.next_url:
            return checkpoint.next_url
        if checkpoint.cursor is not None:
            return self.source.url_for_cursor(checkpoint.cursor)
        # A null cursor checkpoint means the root page was consumed and is the
        # position to poll; re-requesting it is how new data is discovered.
        return self.source.url

    def _drain(self, result: SyncResult, run_id: str, checkpoint: Any) -> None:
        had_checkpoint = checkpoint is not None
        start_cursor = checkpoint.cursor if checkpoint else None
        pages_consumed = checkpoint.pages_consumed if checkpoint else 0
        last_raw_sha256 = checkpoint.last_raw_sha256 if checkpoint else None
        start_url = self._start_url(checkpoint)

        if start_url is None:
            result.status = "no_progress"
            result.committed_through = start_cursor
            result.end_cursor = start_cursor
            return

        seen_hashes: set[str] = set()
        seen_urls: set[str] = set()
        seen_cursors: set[str | None] = (
            {start_cursor} if start_cursor is not None else set()
        )
        cursor = start_cursor
        page_url = start_url
        pulled = 0

        while True:
            if pulled >= self.max_pages:
                raise SourceProtocolError(
                    f"page limit {self.max_pages} reached; provider may be looping")
            pulled += 1

            cycle_url = page_url in seen_urls
            body = self.source.fetch_raw(page_url)
            digest = self.raw_store.save(body)
            page = self.source.parse(body, cursor)
            if page.raw_sha256 != digest:
                raise SourceResponseError("raw evidence digest mismatch")

            seen_urls.add(page_url)
            cursor_out = page.cursor_out

            # 1. Address cycle: the provider linked back to a URL already
            #    fetched in this run.  New records are consumed once; identical
            #    bytes are only ledgered.  The checkpoint is pinned so a re-run
            #    replays the same position and stops rather than walking the
            #    loop.
            if cycle_url:
                repeated = digest in seen_hashes or digest == last_raw_sha256
                if repeated:
                    self._record_nonadvancing_page(
                        result, run_id, page_url, cursor, cursor_out,
                        page.raw_sha256, len(page.records), duplicate=True,
                        pages_consumed=pages_consumed)
                    result.duplicate_pages += 1
                else:
                    self._consume_and_pin(
                        result, run_id, page, page_url, cursor, cursor_out,
                        pages_consumed, checkpoint_cursor=cursor)
                result.status = "cycle_detected"
                result.error = ("pagination cycle detected; checkpoint pinned to the "
                                "last resumable position")
                return

            # 2. Byte-level duplicate: these exact bytes are already consumed,
            #    either by the checkpoint or earlier in this run.  Touch no
            #    rows and leave the checkpoint where it is.
            if digest in seen_hashes or digest == last_raw_sha256:
                self._record_nonadvancing_page(
                    result, run_id, page_url, cursor, cursor_out, page.raw_sha256,
                    len(page.records), duplicate=True, pages_consumed=pages_consumed)
                # A checkpoint already at this position makes the run a plain
                # "nothing new" poll.  A repeat encountered mid-run (or on a
                # first run) is the provider serving a duplicate page.
                idle_poll = had_checkpoint and result.pages == 0
                result.status = "no_new_data" if idle_poll else "duplicate_page"
                result.duplicate_pages += 1
                result.committed_through = cursor
                result.end_cursor = cursor
                return
            seen_hashes.add(digest)

            # 3. Cursor cycle: the advertised next cursor was already visited
            #    in this run, so following it would repeat work.
            if (cursor_out is not None and cursor_out != cursor
                    and cursor_out in seen_cursors):
                self._consume_and_pin(
                    result, run_id, page, page_url, cursor, cursor_out,
                    pages_consumed, checkpoint_cursor=cursor)
                result.status = "cycle_detected"
                result.error = ("pagination cycle detected; checkpoint pinned to the "
                                "last resumable position")
                return

            # 4. Link-only pagination: no cursor field, but the provider links
            #    to the next page.  Consume the page and move to the link while
            #    keeping the (null) cursor pinned to the position for polling.
            if cursor_out is None and page.next_url is not None:
                changed, rejected = self._apply_page(
                    page,
                    run_id=run_id,
                    sequence=pages_consumed + 1,
                    page_url=redact_url(page_url) or "",
                    cursor_in=cursor,
                    cursor_out=None,
                    duplicate=False,
                    checkpoint_cursor=cursor,
                    checkpoint_next_url=_checkpoint_next_url(page.next_url),
                )
                pages_consumed += 1
                result.pages += 1
                result.processed += len(page.records)
                result.changed += changed
                result.rejected += rejected
                result.committed_through = cursor
                result.end_cursor = cursor
                last_raw_sha256 = digest
                page_url = page.next_url
                continue

            # 5. Non-advancing cursor: ``next_cursor`` equals the current one,
            #    or the page is terminal.  New records are consumed once, the
            #    position is pinned, and the run stops — polling the same
            #    position is what discovers newly appended data next time.
            if cursor_out is None or cursor_out == cursor:
                self._consume_and_pin(
                    result, run_id, page, page_url, cursor, cursor_out,
                    pages_consumed, checkpoint_cursor=cursor)
                break

            # 6. Normal advance: records and the new checkpoint commit together.
            changed, rejected = self._apply_page(
                page,
                run_id=run_id,
                sequence=pages_consumed + 1,
                page_url=redact_url(page_url) or "",
                cursor_in=cursor,
                cursor_out=cursor_out,
                duplicate=False,
                checkpoint_cursor=cursor_out,
                checkpoint_next_url=_checkpoint_next_url(page.next_url),
            )
            pages_consumed += 1
            result.pages += 1
            result.processed += len(page.records)
            result.changed += changed
            result.rejected += rejected
            result.committed_through = cursor_out
            result.end_cursor = cursor_out
            last_raw_sha256 = digest
            seen_cursors.add(cursor_out)
            cursor = cursor_out
            page_url = self._next_url(page, cursor_out, page_url)

        result.status = self._terminal_status(result, had_checkpoint, start_cursor)

    def _next_url(self, page: Page, cursor_out: str | None, current_url: str) -> str:
        if page.next_url and safe_to_persist(page.next_url):
            return page.next_url
        if cursor_out is None:
            return current_url
        # Either the provider sent no link or the link would require storing
        # credentials; rebuild a canonical cursor URL instead.
        return self.source.url_for_cursor(cursor_out)

    def _consume_and_pin(
        self,
        result: SyncResult,
        run_id: str,
        page: Page,
        page_url: str,
        cursor_in: str | None,
        cursor_out: str | None,
        pages_consumed: int,
        *,
        checkpoint_cursor: str | None,
    ) -> bool:
        """Consume a non-advancing page's new records once and pin the position.

        Returns True when records were consumed.  The pin stores the URL that
        produced this page (when it is safe to persist) so a later run
        re-requests the identical position; identical bytes are then detected
        as a replay and the run stops instead of walking the loop.  When the
        URL cannot be persisted, the cursor is stored and rebuilt instead.
        """
        if not page.records:
            self._record_nonadvancing_page(
                result, run_id, page_url, cursor_in, cursor_out,
                page.raw_sha256, 0, duplicate=False, pages_consumed=pages_consumed)
            return False
        changed, rejected = self._apply_page(
            page,
            run_id=run_id,
            sequence=pages_consumed + 1,
            page_url=redact_url(page_url) or "",
            cursor_in=cursor_in,
            cursor_out=cursor_out,
            duplicate=False,
            checkpoint_cursor=checkpoint_cursor,
            checkpoint_next_url=_checkpoint_next_url(page_url),
        )
        result.pages += 1
        result.processed += len(page.records)
        result.changed += changed
        result.rejected += rejected
        result.committed_through = checkpoint_cursor
        result.end_cursor = checkpoint_cursor
        return True

    def _terminal_status(
        self, result: SyncResult, had_checkpoint: bool, start_cursor: str | None
    ) -> str:
        if not had_checkpoint:
            return "first_run"
        if result.end_cursor == start_cursor and result.changed == 0 \
                and result.rejected == 0:
            return "no_new_data"
        return "incremental"

    def _apply_page(
        self,
        page: Page,
        *,
        run_id: str,
        sequence: int,
        page_url: str,
        cursor_in: str | None,
        cursor_out: str | None,
        duplicate: bool,
        checkpoint_cursor: str | None,
        checkpoint_next_url: str | None,
    ) -> tuple[int, int]:
        changed = rejected = 0
        with self.store.transaction():
            for raw in page.records:
                try:
                    if self.store.upsert(self.normalizer(raw)):
                        changed += 1
                except (TypeError, ValueError, KeyError) as exc:
                    # Count only newly persisted rejections: replaying an
                    # uncommitted page after a crash must not inflate stats.
                    rejected += int(self.store.reject(raw, str(exc)))
            self.store.record_page(
                stream=self.stream,
                run_id=run_id,
                sequence=sequence,
                page_url=page_url,
                cursor_in=cursor_in,
                cursor_out=cursor_out,
                raw_sha256=page.raw_sha256,
                record_count=len(page.records),
                changed_count=changed,
                rejected_count=rejected,
                duplicate=duplicate,
            )
            # Always persist the checkpoint for a consumed page, even when the
            # cursor is null: the checkpoint row itself is what marks the
            # stream as initialized, which the first-run/first-run-repeat
            # distinction depends on.
            self.store.save_checkpoint(
                self.stream,
                cursor=checkpoint_cursor,
                next_url=checkpoint_next_url,
                last_page_url=page_url,
                last_raw_sha256=page.raw_sha256,
                pages_consumed=sequence,
                run_id=run_id,
            )
        return changed, rejected

    def _record_nonadvancing_page(
        self,
        result: SyncResult,
        run_id: str,
        page_url: str,
        cursor_in: str | None,
        cursor_out: str | None,
        raw_sha256: str,
        record_count: int,
        *,
        duplicate: bool,
        pages_consumed: int,
    ) -> None:
        """Audit a page we deliberately did not consume.

        No observation is written and the checkpoint is untouched; only the
        page ledger notes that the provider served this page.  Raw evidence was
        already stored, so the page is diagnosable without being "consumed".
        """
        with self.store.transaction():
            self.store.record_page(
                stream=self.stream,
                run_id=run_id,
                sequence=pages_consumed + 1,
                page_url=redact_url(page_url) or "",
                cursor_in=cursor_in,
                cursor_out=cursor_out,
                raw_sha256=raw_sha256,
                record_count=record_count,
                changed_count=0,
                rejected_count=0,
                duplicate=duplicate,
            )

    def _finalize(self, result: SyncResult, run_id: str, *, status: str, error: str | None) -> None:
        if error is not None:
            result.error = error
        result.status = status
        try:
            self.store.finish_run(
                run_id,
                status=status,
                end_cursor=result.end_cursor,
                pages=result.pages,
                duplicate_pages=result.duplicate_pages,
                processed=result.processed,
                changed=result.changed,
                rejected=result.rejected,
                error=result.error,
            )
            counts = self.store.counts()
        except Exception:
            # The run summary is diagnostic.  A failure to write it must never
            # mask the original exception or make a committed checkpoint look
            # failed; the next run will mark this run interrupted.
            return
        result.observations = counts["observations"]
        result.rejected_total = counts["rejected"]


def _checkpoint_next_url(next_url: str | None) -> str | None:
    if next_url is None:
        return None
    if not safe_to_persist(next_url):
        # Never persist signed/secret-bearing links; ``url_for_cursor``
        # rebuilds the next request from the stored cursor instead.
        return None
    return next_url


def _describe(exc: BaseException) -> str:
    name = type(exc).__name__
    text = str(exc).strip()
    return f"{name}: {redact_message(text)}" if text else name
