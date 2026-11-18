#!/usr/bin/env python3
"""Auto-assign new Codex App Server threads to projects by exact cwd/root match.

Requires Python 3.11+ and websockets>=14,<17.
Connects to an ALREADY RUNNING app-server via WebSocket or a Unix control socket;
it does not start another Codex app-server process.

The project and project-assignment RPCs are experimental as of Nov 2026.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import ntpath
import os
import posixpath
import time
import uuid
from urllib.parse import urlsplit
from collections.abc import Mapping
from contextlib import suppress
from typing import Any

from websockets.asyncio.client import connect, unix_connect

LOG = logging.getLogger("codex-project-assigner")


class UnsupportedAppServer(RuntimeError):
    """The connected server cannot provide the required experimental APIs."""


class RpcError(Exception):
    def __init__(self, method: str, error: Mapping[str, Any]):
        self.method = method
        self.code = error.get("code")
        self.message = str(error.get("message", "Unknown RPC error"))
        super().__init__(f"{method}: {self.code}: {self.message}")


def normalized_path(path: str) -> str:
    """Compare exact roots; do not assume a remote path exists locally.

    Windows paths are normalized as Windows paths even on POSIX clients.
    Paths through symlinks are *not* equated unless server reports the same path.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("empty path")
    if ntpath.splitdrive(path)[0] or "\\" in path:
        if not ntpath.isabs(path):
            raise ValueError(f"path is not absolute: {path!r}")
        return ntpath.normcase(ntpath.normpath(path))
    if not posixpath.isabs(path):
        raise ValueError(f"path is not absolute: {path!r}")
    return posixpath.normpath(path)


def project_name(cwd: str) -> str:
    path_module = ntpath if ntpath.splitdrive(cwd)[0] or "\\" in cwd else posixpath
    return path_module.basename(path_module.normpath(cwd)) or cwd


class JsonRpcClient:
    """Multiplexes requests and selected notifications over one WebSocket."""

    NOTIFICATIONS = {"thread/started", "project/changed"}

    def __init__(self, websocket: Any, *, timeout: float = 20.0):
        self.ws = websocket
        self.timeout = timeout
        self._counter = 0
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=4096)
        self._reader_task: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()

    def start(self) -> None:
        self._reader_task = asyncio.create_task(self._read_loop(), name="codex-rpc-reader")

    async def close(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader_task

    async def _send(self, message: dict[str, Any]) -> None:
        async with self._send_lock:
            await self.ws.send(json.dumps(message, separators=(",", ":")))

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        await self._send({"method": method, "params": params or {}})

    async def call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._counter += 1
        request_id = self._counter
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send({"id": request_id, "method": method, "params": params or {}})
            response = await asyncio.wait_for(future, timeout=self.timeout)
        finally:
            self._pending.pop(request_id, None)
        if "error" in response:
            raise RpcError(method, response["error"])
        result = response.get("result")
        if not isinstance(result, dict):
            raise ValueError(f"{method}: unexpected result: {result!r}")
        return result

    async def initialize(self) -> None:
        await self.call(
            "initialize",
            {
                "clientInfo": {
                    "name": "cwd_project_assigner",
                    "title": "CWD Project Assigner",
                    "version": "1.0.0",
                },
                "capabilities": {"experimentalApi": True},
            },
        )
        await self.notify("initialized")

    async def _read_loop(self) -> None:
        try:
            async for frame in self.ws:
                try:
                    msg = json.loads(frame)
                except (ValueError, TypeError):
                    LOG.warning("Skipping invalid JSON-RPC frame")
                    continue
                if not isinstance(msg, dict):
                    continue
                if "id" in msg and "method" not in msg:
                    future = self._pending.get(msg["id"])
                    if future is not None and not future.done():
                        future.set_result(msg)
                elif "method" in msg and "id" in msg:
                    # Fail closed on unexpected server-initiated requests. Never
                    # automatically approve commands or user input requests.
                    await self._send({
                        "id": msg["id"],
                        "error": {"code": -32601, "message": "Method not supported"},
                    })
                elif msg.get("method") in self.NOTIFICATIONS:
                    try:
                        self.events.put_nowait(msg)
                    except asyncio.QueueFull:
                        LOG.error("Notification queue overflow; periodic reconciliation will recover")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("WebSocket reader exited: %s", exc)
        finally:
            for pending in self._pending.values():
                if not pending.done():
                    pending.set_exception(ConnectionError("App Server connection closed"))
            with suppress(asyncio.QueueFull):
                self.events.put_nowait({"method": "__disconnected"})


class ProjectAssigner:
    """One-writer, cache-first CWD matching with serialization per CWD."""

    def __init__(self, rpc: Any):
        self.rpc = rpc
        self.projects: list[dict[str, Any]] = []
        self.cache_valid = False
        self._project_mutex = asyncio.Lock()
        self._cwd_locks: dict[str, asyncio.Lock] = {}
        self._thread_locks: dict[str, asyncio.Lock] = {}

    def invalidate_projects(self) -> None:
        self.cache_valid = False

    async def refresh_projects(self) -> None:
        async with self._project_mutex:
            await self._refresh_projects_locked()

    async def _refresh_projects_locked(self) -> None:
        projects: list[dict[str, Any]] = []
        cursor: str | None = None
        cursors_seen: set[str] = set()
        while True:
            params: dict[str, Any] = {"limit": 100}
            if cursor is not None:
                params["cursor"] = cursor
            response = await self.rpc.call("project/list", params)
            page = response.get("data")
            if not isinstance(page, list):
                raise ValueError("project/list returned invalid data")
            projects.extend(page)
            cursor = response.get("nextCursor")
            if cursor is None:
                break
            if not isinstance(cursor, str) or cursor in cursors_seen:
                raise ValueError("project/list returned invalid/repeated nextCursor")
            cursors_seen.add(cursor)
        self.projects = projects
        self.cache_valid = True
        LOG.info("Loaded %d projects", len(projects))

    def _find(self, cwd: str) -> dict[str, Any] | None:
        target = normalized_path(cwd)
        matches: list[dict[str, Any]] = []
        for project in self.projects:
            for root in project.get("roots", []):
                path = root.get("path") if isinstance(root, dict) else None
                try:
                    if path and normalized_path(path) == target:
                        matches.append(project)
                        break
                except ValueError:
                    LOG.warning("Ignoring malformed root path from project %s", project.get("id"))
        if len(matches) > 1:
            LOG.warning("Multiple projects match %s; using first project %s", cwd, matches[0].get("id"))
        return matches[0] if matches else None

    async def _get_or_create_project(self, cwd: str) -> dict[str, Any]:
        # Caller holds the per-cwd lock. Project mutex guards cache updates/creates
        # and prevents different cwd operations from seeing half-refreshed data.
        async with self._project_mutex:
            if not self.cache_valid:
                await self._refresh_projects_locked()
            existing = self._find(cwd)
            if existing:
                return existing
            # Required cache miss refresh, even when cache was already loaded.
            await self._refresh_projects_locked()
            existing = self._find(cwd)
            if existing:
                return existing
            params = {
                "name": project_name(cwd),
                "roots": [{"path": cwd}],
                "idempotencyKey": str(uuid.uuid4()),
            }
            # Reuse the SAME idempotency key if the response was lost.
            try:
                response = await self.rpc.call("project/create", params)
            except (asyncio.TimeoutError, ConnectionError):
                await self._refresh_projects_locked()
                existing = self._find(cwd)
                if existing:
                    return existing
                response = await self.rpc.call("project/create", params)
            project = response.get("project")
            if not isinstance(project, dict) or not project.get("id"):
                raise ValueError("project/create returned no project ID")
            self.projects.append(project)
            LOG.info("Created project %s for cwd=%s", project["id"], cwd)
            return project

    async def _read_thread(self, thread_id: str) -> dict[str, Any] | None:
        # New thread snapshots may not be visible in durable storage immediately.
        for attempt in range(5):
            try:
                response = await self.rpc.call(
                    "thread/read", {"threadId": thread_id, "includeTurns": False}
                )
                thread = response.get("thread")
                if not isinstance(thread, dict):
                    raise ValueError("thread/read returned no thread")
                return thread
            except RpcError as exc:
                if "thread not found" not in exc.message.lower():
                    raise
                if attempt < 4:
                    await asyncio.sleep(0.25 * (2**attempt))
        LOG.warning("Thread %s not readable yet; reconciliation will retry", thread_id)
        return None

    async def process_thread(self, thread: Mapping[str, Any]) -> None:
        tid = thread.get("id")
        if not isinstance(tid, str) or not tid:
            return
        if thread.get("projectId") or thread.get("ephemeral") is True:
            return
        # Serializes duplicate deliveries for the same thread.
        lock = self._thread_locks.setdefault(tid, asyncio.Lock())
        async with lock:
            current = await self._read_thread(tid)
            if current is None or current.get("projectId") or current.get("ephemeral") is True:
                return
            cwd = current.get("cwd") or thread.get("cwd")
            try:
                key = normalized_path(cwd)
            except (TypeError, ValueError):
                LOG.warning("Thread %s has no valid absolute cwd: %r", tid, cwd)
                return
            cwd_lock = self._cwd_locks.setdefault(key, asyncio.Lock())
            async with cwd_lock:
                # Recheck authoritative assignment while holding lock.
                current = await self._read_thread(tid)
                if current is None or current.get("projectId") or current.get("ephemeral") is True:
                    return
                latest_cwd = current.get("cwd") or cwd
                if normalized_path(latest_cwd) != key:
                    LOG.warning("Thread %s changed cwd while assigning; deferred to reconciliation", tid)
                    return
                project = await self._get_or_create_project(latest_cwd)
                # Narrow the TOCTOU window before the non-conditional update.
                # The API does not expose a compare-and-set for projectId.
                before_write = await self._read_thread(tid)
                if before_write is None or before_write.get("projectId"):
                    return
                if normalized_path(before_write.get("cwd") or latest_cwd) != key:
                    LOG.warning("Thread %s cwd changed before update; skipped", tid)
                    return
                try:
                    result = await self.rpc.call(
                        "thread/metadata/update", {"threadId": tid, "projectId": project["id"]}
                    )
                except RpcError as exc:
                    if "project not found" not in exc.message.lower():
                        raise
                    # Stale project cache: invalidate and retry from server state.
                    self.invalidate_projects()
                    project = await self._get_or_create_project(latest_cwd)
                    result = await self.rpc.call(
                        "thread/metadata/update", {"threadId": tid, "projectId": project["id"]}
                    )
                actual_id = result.get("thread", {}).get("projectId")
                if actual_id != project["id"]:
                    raise ValueError(f"Project update not verified for thread {tid}: {actual_id!r}")
                LOG.info("Assigned thread %s -> project %s (cwd=%s)", tid, actual_id, latest_cwd)

    async def reconcile_since(self, cutoff_unix_seconds: int) -> None:
        """Recover missed events; do NOT rewrite older, historical unassigned threads."""
        cursor: str | None = None
        cursors_seen: set[str] = set()
        while True:
            params: dict[str, Any] = {"projectId": None, "limit": 100}
            if cursor is not None:
                params["cursor"] = cursor
            response = await self.rpc.call("thread/list", params)
            threads = response.get("data")
            if not isinstance(threads, list):
                raise ValueError("thread/list returned invalid data")
            old_thread_reached = False
            for thread in threads:
                if not isinstance(thread, dict):
                    continue
                created_at = thread.get("createdAt")
                if not isinstance(created_at, (int, float)):
                    continue
                if created_at < cutoff_unix_seconds:
                    old_thread_reached = True
                    break
                await self.process_thread(thread)
            if old_thread_reached:
                return
            cursor = response.get("nextCursor")
            if cursor is None:
                return
            if not isinstance(cursor, str) or cursor in cursors_seen:
                raise ValueError("thread/list returned invalid/repeated nextCursor")
            cursors_seen.add(cursor)


async def run_connection(
    ws: Any, *, request_timeout: float, lookback_cutoff: int, reconcile_interval: float
) -> None:
    rpc = JsonRpcClient(ws, timeout=request_timeout)
    rpc.start()
    background: set[asyncio.Task[Any]] = set()
    limiter = asyncio.Semaphore(16)
    reconciliation: asyncio.Task[Any] | None = None
    try:
        await rpc.initialize()
        assigner = ProjectAssigner(rpc)
        try:
            await assigner.refresh_projects()
        except RpcError as exc:
            if exc.code == -32601 or "unsupported" in exc.message.lower() or "experimentalapi" in exc.message.lower():
                raise UnsupportedAppServer(
                    "This App Server does not support project/list (experimental). "
                    "Upgrade Codex or enable the project-capable thread store."
                ) from exc
            raise

        async def assign_with_log(thread: dict[str, Any]) -> None:
            async with limiter:
                try:
                    await assigner.process_thread(thread)
                except Exception:
                    LOG.exception("Could not assign thread %s", thread.get("id"))

        async def reconcile_loop() -> None:
            while True:
                try:
                    await assigner.reconcile_since(lookback_cutoff)
                except Exception:
                    LOG.exception("Reconciliation failed")
                await asyncio.sleep(reconcile_interval)

        reconciliation = asyncio.create_task(reconcile_loop(), name="codex-reconcile")
        LOG.info("Listening for thread/started and project/changed")
        while True:
            if rpc._reader_task and rpc._reader_task.done():
                raise ConnectionError("App Server connection lost")
            try:
                event = await asyncio.wait_for(rpc.events.get(), timeout=1)
            except asyncio.TimeoutError:
                continue
            method = event.get("method")
            if method == "__disconnected":
                raise ConnectionError("App Server connection lost")
            if method == "project/changed":
                assigner.invalidate_projects()
            elif method == "thread/started":
                thread = event.get("params", {}).get("thread")
                if isinstance(thread, dict):
                    task = asyncio.create_task(assign_with_log(thread))
                    background.add(task)
                    task.add_done_callback(background.discard)
    finally:
        if reconciliation is not None:
            reconciliation.cancel()
            with suppress(asyncio.CancelledError):
                await reconciliation
        for task in background:
            task.cancel()
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        await rpc.close()


async def main_async(args: argparse.Namespace) -> None:
    cutoff = int(time.time()) - args.lookback_seconds
    delay = 1.0
    while True:
        try:
            headers = None
            token = os.environ.get(args.token_env)
            if token:
                headers = {"Authorization": f"Bearer {token}"}
            kwargs = {"additional_headers": headers, "max_size": 8 * 1024 * 1024}
            if args.unix_socket:
                connection = unix_connect(args.unix_socket, uri="ws://localhost/", **kwargs)
            else:
                connection = connect(args.url, **kwargs)
            async with connection as ws:
                LOG.info("Connected to existing Codex App Server")
                delay = 1.0
                await run_connection(
                    ws,
                    request_timeout=args.request_timeout,
                    lookback_cutoff=cutoff,
                    reconcile_interval=args.reconcile_interval,
                )
        except asyncio.CancelledError:
            raise
        except UnsupportedAppServer:
            raise
        except Exception as exc:
            LOG.error("Disconnected or failed: %s; retrying in %.1fs", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    endpoint = parser.add_mutually_exclusive_group()
    endpoint.add_argument("--url", default="ws://127.0.0.1:4500", help="Existing app-server WS URL")
    endpoint.add_argument("--unix-socket", help="Path of an existing app-server Unix socket")
    parser.add_argument("--token-env", default="CODEX_APP_SERVER_TOKEN", help="Bearer token env variable")
    parser.add_argument("--request-timeout", type=float, default=20)
    parser.add_argument("--reconcile-interval", type=float, default=60)
    parser.add_argument("--lookback-seconds", type=int, default=300)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    if not args.unix_socket:
        endpoint_url = urlsplit(args.url)
        if endpoint_url.scheme not in ("ws", "wss") or not endpoint_url.hostname:
            parser.error("--url must use ws:// or wss://")
        if endpoint_url.scheme == "ws" and endpoint_url.hostname not in (
            "localhost", "127.0.0.1", "::1"
        ):
            parser.error("non-local endpoints require wss:// (or use a local SSH tunnel)")
    if args.request_timeout <= 0 or args.reconcile_interval <= 0 or args.lookback_seconds < 0:
        parser.error("timeouts/interval must be positive; lookback must be >= 0")
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass
    except UnsupportedAppServer as exc:
        parser.exit(2, f"{exc}\n")


if __name__ == "__main__":
    main()
