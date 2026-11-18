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

### Backfill existing threads once

```bash
uv run python codex_project_assigner.py --url ws://127.0.0.1:4500 --backfill-once
uv run python codex_project_assigner.py --unix-socket /path/to/codex-app-server.sock --backfill-once
```

Backfill scans the unassigned history exposed by `thread/list`, including archived threads, with no age cutoff. It reuses or creates projects using the same exact working-directory matching as the watcher. It preserves existing assignments and skips ephemeral threads and threads without a valid absolute working directory.

The command prints assigned, skipped, and failed counts and exits. A listing or processing failure causes a non-zero exit status. You can rerun the command to pick up remaining unassigned threads. The same endpoint and authentication options apply.

`--lookback-seconds` sets the watcher's startup cutoff for recovering missed threads, and `--reconcile-interval` controls how often the watcher checks for them. Backfill ignores both options and exits after one pass through the history.

## Options

- `--backfill-once`: Assign existing unassigned threads of any age, then exit.
- `--lookback-seconds 300`: In watcher mode, also recover unassigned threads created in the five minutes before startup.
- `--reconcile-interval 60`: In watcher mode, use `thread/list` every 60 seconds to recover threads missed during disconnections or event loss.
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

The suite covers assignment, historical backfill, and a simulated WebSocket connection. Connection testing against a real Codex App Server has not been performed.

## Protocol References

- https://learn.chatgpt.com/docs/app-server
- https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/project.rs
- https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs
- https://github.com/openai/codex/issues/54482
