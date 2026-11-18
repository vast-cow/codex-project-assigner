"""Offline unit tests; no Codex instance is required."""
import asyncio
import copy
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from codex_project_assigner import ProjectAssigner, normalized_path, project_name


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
                if t.get("projectId") is None
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


if __name__ == "__main__":
    unittest.main(verbosity=2)

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
