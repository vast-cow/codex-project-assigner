# Codex App Server: Automatic Project Assignment by Working Directory

Connect to an existing **Codex App Server listening on a WebSocket or Unix socket** using Python, and monitor `thread/started` events. If a new thread has no `projectId`, compare its `cwd` against project roots for an exact match, in this order:

1. Search the project cache.
2. If no match is found, fetch all pages from `project/list` and search them.
3. If no match is found, create a project with `project/create`.
4. Check the current state with `thread/read`, then assign the project with `thread/metadata/update`.

## Requirements

- Python 3.11 or later
- `uv` (`websockets>=14,<17` is installed by `uv sync`)
- Codex App Server with support for the Experimental API
- An endpoint that allows connections to the existing App Server over **WebSocket or Unix socket** (an existing stdio-only process cannot be attached to directly)

## Run

```bash
uv sync
uv run python codex_project_assigner.py --url ws://127.0.0.1:4500
```

To start a Codex App Server listening on WebSocket, use `codex app-server --listen ws://127.0.0.1:4500`. This command starts a **new server**. To reuse an App Server that is already running, pass its endpoint with `--url`.

If a Unix socket endpoint is available:

```bash
uv run python codex_project_assigner.py --unix-socket /path/to/codex-app-server.sock
```

For an authenticated WebSocket, set the token in the `CODEX_APP_SERVER_TOKEN` environment variable instead of putting it directly in the command-line arguments. Protect non-local connections with `wss://` or an SSH tunnel.

## Options

- `--lookback-seconds 300`: Also recover unassigned threads created in the five minutes before startup. Older existing threads are not modified.
- `--reconcile-interval 60`: Every 60 seconds, use `thread/list` to recover threads missed during disconnections or event loss.
- `--request-timeout 20`: JSON-RPC request timeout, in seconds.
- `--log-level DEBUG`: Enable detailed logging.

## Implementation Notes

- Matching compares `thread.cwd` with each `project.roots[].path`. Paths such as `/foo/../bar` and trailing slashes are normalized. Parent-child directory relationships do not imply a match. The same directory accessed through a symbolic link is treated as different when the root strings differ.
- Multiple threads with the same `cwd` are serialized with `asyncio.Lock` to prevent duplicate project creation within one process. **Do not run multiple watcher processes concurrently.** The server API has no unique constraint on `cwd` or compare-and-set, so duplicate creation cannot be fully prevented across processes.
- `project/create` requires an `idempotencyKey`. Use the same key when retrying the same operation after a timeout.
- The current state is checked with `thread/read`, but `thread/metadata/update` does not support conditional updates (compare-and-set). Concurrent changes by another client therefore cannot be fully prevented.
- Threads with `ephemeral: true` are excluded because they cannot be assigned to a project persistently.
- The cache is invalidated on `project/changed`. All pages are read when `project/list` is paginated.
- Public bug reports describe cases where project management in the Desktop app and App Server project metadata are not fully synchronized. A changed `projectId` on the App Server may not appear immediately in the Desktop sidebar, or may later be reverted.

## Tests

```bash
uv run python -m unittest -v test_codex_project_assigner.py
```

Eleven unit and simulated WebSocket integration test cases have been run. Connection testing against a real Codex App Server has not been performed.

## Protocol References

- https://learn.chatgpt.com/docs/app-server
- https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/project.rs
- https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs
- https://github.com/openai/codex/issues/54482
