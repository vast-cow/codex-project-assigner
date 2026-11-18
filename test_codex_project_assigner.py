"""Offline unit tests; no Codex instance is required."""
import asyncio
import copy
import io
import json
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from codex_project_assigner import ProjectAssigner, RpcError, main, normalized_path, project_name


class FakeRpc:
    def __init__(self, *, projects=(), threads=()):
        self.projects = copy.deepcopy(list(projects))
        self.threads = {t["id"]: copy.deepcopy(t) for t in threads}
        self.calls = []
        self.page_size = 2
        self.listed = 0
        self.created = 0
        self.updated = 0

    async def call(self, method, params=None):
        params = dict(params or {})
        self.calls.append((method, params))
        await asyncio.sleep(0)
        if method == "project/list":
            self.listed += 1
            offset = int(params.get("cursor", "0"))
            count = min(params.get("limit", 100), self.page_size)
            items = self.projects[offset:offset+count]
            next_offset = offset + count
            return {
                "data": copy.deepcopy(items),
                "nextCursor": str(next_offset) if next_offset < len(self.projects) else None,
            }
        if method == "project/create":
            self.created += 1
            assert params.get("idempotencyKey")
            assert isinstance(params["roots"], list)
            project = {
                "id": f"p-{self.created}",
                "name": params["name"],
                "roots": params["roots"],
            }
            self.projects.append(project)
            return {"project": copy.deepcopy(project)}
        if method == "thread/read":
            return {"thread": copy.deepcopy(self.threads[params["threadId"]])}
        if method == "thread/metadata/update":
            self.updated += 1
            thread = self.threads[params["threadId"]]
            if not any(p["id"] == params["projectId"] for p in self.projects):
                raise ValueError("unknown project")
            thread["projectId"] = params["projectId"]
            return {"thread": copy.deepcopy(thread)}
        if method == "thread/list":
            threads = [
                t for t in sorted(self.threads.values(), key=lambda t: t["createdAt"], reverse=True)
                if t.get("projectId") is None and t.get("archived", False) == params.get("archived", False)
            ]
            offset = int(params.get("cursor", "0"))
            count = min(params.get("limit", 100), self.page_size)
            items = threads[offset:offset+count]
            next_offset = offset+count
            return {
                "data": copy.deepcopy(items),
                "nextCursor": str(next_offset) if next_offset < len(threads) else None,
            }
        raise ValueError("Unknown method: " + method)


def thread(tid, cwd="/repos/app", *, project_id=None, created_at=1000, ephemeral=False):
    return {
        "id": tid, "cwd": cwd, "projectId": project_id,
        "createdAt": created_at, "ephemeral": ephemeral,
    }


def project(pid, root):
    return {"id": pid, "name": "Example", "roots": [{"path": root}]}


class AssignerTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_hit_no_refresh_or_creation(self):
        rpc = FakeRpc(projects=[project("existing", "/repos/app")], threads=[thread("t1")])
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.threads["t1"]["projectId"], "existing")
        self.assertEqual(rpc.created, 0)
        self.assertEqual(rpc.listed, 1)
        self.assertEqual(rpc.updated, 1)

    async def test_cache_miss_refresh_finds_external_project(self):
        rpc = FakeRpc(threads=[thread("t1")])
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        rpc.projects.append(project("external", "/repos/app"))
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.threads["t1"]["projectId"], "external")
        self.assertEqual(rpc.created, 0)
        self.assertGreaterEqual(rpc.listed, 2)

    async def test_cache_miss_refresh_then_create(self):
        rpc = FakeRpc(threads=[thread("t1", "/repos/new")])
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.created, 1)
        self.assertEqual(rpc.threads["t1"]["projectId"], "p-1")
        self.assertEqual(rpc.projects[0]["roots"], [{"path": "/repos/new"}])
        self.assertEqual(rpc.projects[0]["name"], "new")

    async def test_two_threads_same_cwd_create_once(self):
        rpc = FakeRpc(threads=[thread("t1"), thread("t2")])
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        await asyncio.gather(
            assigner.process_thread(rpc.threads["t1"]),
            assigner.process_thread(rpc.threads["t2"]),
        )
        self.assertEqual(rpc.created, 1)
        self.assertEqual(rpc.updated, 2)
        self.assertEqual(rpc.threads["t1"]["projectId"], "p-1")
        self.assertEqual(rpc.threads["t2"]["projectId"], "p-1")

    async def test_does_not_override_already_assigned(self):
        rpc = FakeRpc(threads=[thread("t1", project_id="by-user")])
        assigner = ProjectAssigner(rpc)
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.updated, 0)
        # Also test assignment appearing after event publication.
        await assigner.process_thread({"id": "t1", "projectId": None, "cwd": "/repos/app"})
        self.assertEqual(rpc.updated, 0)

    async def test_ephemeral_threads_are_not_mutated(self):
        rpc = FakeRpc(threads=[thread("t1", ephemeral=True)])
        assigner = ProjectAssigner(rpc)
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.created, 0)
        self.assertEqual(rpc.updated, 0)

    async def test_multiple_roots_and_paginated_projects(self):
        rpc = FakeRpc(
            projects=[
                project("one", "/other1"),
                project("two", "/other2"),
                {"id": "target", "name": "Target", "roots": [{"path": "/other3"}, {"path": "/repos/app/"}]},
            ],
            threads=[thread("t1")],
        )
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.threads["t1"]["projectId"], "target")
        self.assertEqual(rpc.created, 0)
        self.assertEqual(rpc.listed, 2)

    async def test_reconcile_recent_only(self):
        rpc = FakeRpc(threads=[
            thread("new1", "/repos/one", created_at=300),
            thread("new2", "/repos/two", created_at=250),
            thread("old", "/repos/old", created_at=100),
        ])
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        await assigner.reconcile_since(200)
        self.assertEqual(rpc.threads["old"]["projectId"], None)
        self.assertEqual(rpc.created, 2)
        self.assertIsNotNone(rpc.threads["new1"]["projectId"])
        self.assertIsNotNone(rpc.threads["new2"]["projectId"])

    async def test_invalidated_cache(self):
        rpc = FakeRpc(projects=[project("old", "/repos/app")], threads=[thread("t1")])
        assigner = ProjectAssigner(rpc)
        await assigner.refresh_projects()
        rpc.projects[:] = [project("new", "/repos/app")]
        assigner.invalidate_projects()
        await assigner.process_thread(rpc.threads["t1"])
        self.assertEqual(rpc.threads["t1"]["projectId"], "new")

    def test_path_normalization(self):
        self.assertEqual(normalized_path("/repos/x/../app/"), "/repos/app")
        self.assertEqual(normalized_path("C:\\Repos\\App\\"), "c:\\repos\\app")
        self.assertEqual(project_name("/repo/myproject/"), "myproject")
        self.assertEqual(project_name("C:\\Repos\\App\\"), "App")
        with self.assertRaises(ValueError):
            normalized_path("relative/path")


class BackfillTests(unittest.IsolatedAsyncioTestCase):
    async def run_backfill(self, rpc):
        output = io.StringIO()
        with redirect_stdout(output):
            status = await ProjectAssigner(rpc, backfill=True).backfill_once()
        return status, output.getvalue().strip()

    async def test_lists_all_pages_before_assigning_threads_of_any_age(self):
        rpc = FakeRpc(
            projects=[project("existing", "/repos/app/")],
            threads=[
                thread("older", "/repos/x/../app", created_at=10),
                thread("oldest", "/repos/app", created_at=1),
                thread("historical", "/repos/new", created_at=20),
                thread("shared", "/repos/new/", created_at=30),
                dict(thread("archived", created_at=5), archived=True),
                thread("no-timestamp", created_at=0),
            ],
        )
        original_call = rpc.call

        async def call(method, params):
            response = await original_call(method, params)
            if method == "thread/list" and params.get("cursor") == "2":
                response["data"].append({"id": "shared"})
            if method == "thread/list" and not response["nextCursor"]:
                for item in response["data"]:
                    if item["id"] == "no-timestamp":
                        item.pop("createdAt")
            return response

        rpc.call = call
        self.assertEqual(await self.run_backfill(rpc), (
            0, "Backfill summary: assigned=6, skipped=0, failed=0",
        ))
        self.assertEqual(rpc.created, 1)
        self.assertEqual(rpc.updated, 6)
        for tid in ("older", "oldest", "archived", "no-timestamp"):
            self.assertEqual(rpc.threads[tid]["projectId"], "existing")
        for tid in ("historical", "shared"):
            self.assertEqual(rpc.threads[tid]["projectId"], "p-1")
        listing = [params for method, params in rpc.calls if method == "thread/list"]
        self.assertEqual([(p["archived"], p.get("cursor")) for p in listing], [
            (False, None), (False, "2"), (False, "4"), (True, None),
        ])
        self.assertTrue(all(p["projectId"] is None and p["modelProviders"] == [] for p in listing))
        self.assertTrue(all("exec" in p["sourceKinds"] for p in listing))
        self.assertEqual([method for method, _ in rpc.calls[:4]], ["thread/list"] * 4)

    async def test_skips_assigned_ephemeral_and_invalid_cwd_and_can_rerun(self):
        rpc = FakeRpc(threads=[
            thread("eligible"),
            thread("assigned", project_id="by-user"),
            thread("ephemeral", ephemeral=True),
            thread("relative", cwd="repos/app"),
            thread("missing", cwd=None),
        ])
        with self.assertLogs("codex-project-assigner", "WARNING"):
            self.assertEqual(await self.run_backfill(rpc), (
                0, "Backfill summary: assigned=1, skipped=3, failed=0",
            ))
            self.assertEqual(await self.run_backfill(rpc), (
                0, "Backfill summary: assigned=0, skipped=3, failed=0",
            ))
        self.assertEqual(rpc.threads["assigned"]["projectId"], "by-user")
        self.assertEqual(rpc.threads["eligible"]["projectId"], "p-1")
        self.assertEqual(rpc.created, 1)
        self.assertEqual(rpc.updated, 1)

    async def test_rechecks_metadata_after_project_lookup(self):
        changes = [
            {"projectId": "by-user"},
            {"ephemeral": True},
            {"cwd": None},
            {"cwd": "/repos/elsewhere"},
        ]
        for change in changes:
            with self.subTest(change=change):
                rpc = FakeRpc(projects=[project("existing", "/repos/app")], threads=[thread("t1")])
                original_call = rpc.call

                async def call(method, params):
                    if method == "project/list":
                        rpc.threads["t1"].update(change)
                    return await original_call(method, params)

                rpc.call = call
                self.assertEqual(await self.run_backfill(rpc), (
                    0, "Backfill summary: assigned=0, skipped=1, failed=0",
                ))
                self.assertEqual(rpc.updated, 0)

    async def test_rechecks_metadata_after_stale_project_failure(self):
        rpc = FakeRpc(projects=[project("existing", "/repos/app")], threads=[thread("t1")])
        original_call = rpc.call

        async def call(method, params):
            if method == "thread/metadata/update":
                rpc.updated += 1
                rpc.projects[:] = [project("replacement", "/repos/app")]
                rpc.threads["t1"]["projectId"] = "by-user"
                raise RpcError(method, {"message": "project not found"})
            return await original_call(method, params)

        rpc.call = call
        self.assertEqual(await self.run_backfill(rpc), (
            0, "Backfill summary: assigned=0, skipped=1, failed=0",
        ))
        self.assertEqual(rpc.updated, 1)
        self.assertEqual(rpc.threads["t1"]["projectId"], "by-user")

    async def test_logs_read_failure_and_continues(self):
        rpc = FakeRpc(threads=[thread("unreadable"), thread("eligible")])
        original_call = rpc.call

        async def call(method, params):
            if method == "thread/read" and params["threadId"] == "unreadable":
                raise RpcError(method, {"message": "thread not found"})
            return await original_call(method, params)

        rpc.call = call
        with self.assertLogs("codex-project-assigner", "ERROR") as logs:
            self.assertEqual(await self.run_backfill(rpc), (
                1, "Backfill summary: assigned=1, skipped=0, failed=1",
            ))
        self.assertIn("unreadable", logs.output[0])
        self.assertEqual(rpc.updated, 1)

    async def test_repeated_cursor_fails_without_writes(self):
        rpc = FakeRpc(threads=[thread("t1")])
        original_call = rpc.call

        async def call(method, params):
            response = await original_call(method, params)
            if method == "thread/list":
                response["nextCursor"] = "0"
            return response

        rpc.call = call
        with self.assertLogs("codex-project-assigner", "ERROR") as logs:
            self.assertEqual(await self.run_backfill(rpc), (
                1, "Backfill summary: assigned=0, skipped=0, failed=0",
            ))
        self.assertIn("invalid/repeated nextCursor", logs.output[0])
        self.assertEqual(rpc.updated, 0)


class BackfillCliTests(unittest.TestCase):
    def test_cli_connects_once_and_ignores_events(self):
        rpc = FakeRpc(threads=[thread("historical", created_at=1)])

        class WebSocket:
            async def __aenter__(self):
                self.messages = asyncio.Queue()
                return self

            async def __aexit__(self, *exc):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                return await self.messages.get()

            async def send(self, message):
                request = json.loads(message)
                method = request["method"]
                if method == "initialized":
                    self.messages.put_nowait(json.dumps({
                        "method": "thread/started",
                        "params": {"thread": thread("event-only")},
                    }))
                    return
                result = {} if method == "initialize" else await rpc.call(method, request["params"])
                self.messages.put_nowait(json.dumps({"id": request["id"], "result": result}))

        url = "ws://127.0.0.1:4500"
        output = io.StringIO()
        with (
            patch("sys.argv", ["assigner", "--url", url, "--backfill-once", "--token-env", "TEST_TOKEN"]),
            patch.dict(os.environ, {"TEST_TOKEN": "test-token"}),
            patch("codex_project_assigner.connect", return_value=WebSocket()) as connect_mock,
            redirect_stdout(output),
            self.assertRaises(SystemExit) as result,
        ):
            main()
        self.assertEqual(result.exception.code, 0)
        connect_mock.assert_called_once_with(
            url, additional_headers={"Authorization": "Bearer test-token"}, max_size=8 * 1024 * 1024,
        )
        self.assertEqual(output.getvalue().strip(), "Backfill summary: assigned=1, skipped=0, failed=0")
        self.assertFalse(any(params.get("threadId") == "event-only" for _, params in rpc.calls))
        self.assertEqual(sum(method == "thread/list" for method, _ in rpc.calls), 2)

    def test_cli_exits_on_connection_failure(self):
        with (
            patch("sys.argv", ["assigner", "--backfill-once"]),
            patch("codex_project_assigner.connect", side_effect=OSError("connection failed")) as connect_mock,
            self.assertLogs("codex-project-assigner", "ERROR"),
            self.assertRaises(SystemExit) as result,
        ):
            main()
        self.assertEqual(result.exception.code, 1)
        connect_mock.assert_called_once()

# Separate end-to-end transport smoke test, simulating an already-running server.
from codex_project_assigner import run_connection
from websockets.asyncio.server import serve
from websockets.asyncio.client import connect


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_websocket_handshake_and_assignment_event(self):
        assigned = asyncio.Event()
        rpc_calls = []
        backend = thread("thread-1", "/workspace/app", created_at=1000)
        projects = []

        async def server_handler(ws):
            async for raw in ws:
                request = __import__("json").loads(raw)
                method = request.get("method")
                rpc_calls.append(method)
                if "id" not in request:
                    if method == "initialized":
                        await ws.send(__import__("json").dumps({
                            "method": "thread/started", "params": {"thread": dict(backend)}
                        }))
                    continue
                params = request["params"]
                if method == "initialize":
                    self.assertTrue(params["capabilities"]["experimentalApi"])
                    result = {"userAgent": "test"}
                elif method == "project/list":
                    result = {"data": list(projects), "nextCursor": None}
                elif method == "thread/list":
                    result = {"data": [], "nextCursor": None}
                elif method == "thread/read":
                    result = {"thread": dict(backend)}
                elif method == "project/create":
                    project = {"id": "created", "name": params["name"], "roots": params["roots"]}
                    projects.append(project)
                    result = {"project": project}
                elif method == "thread/metadata/update":
                    backend["projectId"] = params["projectId"]
                    result = {"thread": dict(backend)}
                    assigned.set()
                else:
                    self.fail("Unexpected RPC: " + method)
                await ws.send(__import__("json").dumps({"id": request["id"], "result": result}))

        async with serve(server_handler, "127.0.0.1", 0) as mock_server:
            port = mock_server.sockets[0].getsockname()[1]
            async with connect(f"ws://127.0.0.1:{port}") as ws:
                task = asyncio.create_task(run_connection(
                    ws, request_timeout=2, lookback_cutoff=200, reconcile_interval=10,
                ))
                try:
                    await asyncio.wait_for(assigned.wait(), timeout=3)
                    self.assertEqual(backend["projectId"], "created")
                    self.assertIn("thread/metadata/update", rpc_calls)
                finally:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task


if __name__ == "__main__":
    unittest.main(verbosity=2)
