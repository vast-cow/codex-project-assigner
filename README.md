# Codex App Server: cwd ベースの Project 自動割り当て

既存の **WebSocket / Unix socket 待受中の Codex App Server** に Python で接続し、`thread/started` を監視します。新規 thread に `projectId` がなければ、次の順で `cwd` と project roots を完全一致比較します。

1. project キャッシュで探す
2. 一致しなければ `project/list` の全ページを取得して探す
3. 一致しなければ `project/create` で新規作成する
4. `thread/read` で現状態を確認して `thread/metadata/update` で割り当てる

## 要件

- Python 3.11 以上
- `uv`（`websockets>=14,<17` は `uv sync` でインストール）
- Experimental API に対応した Codex App Server
- 既存の App Server に接続可能な **WebSocket / Unix socket endpoint** があること（stdio-only の既存プロセスには直接 attach できません）

## 実行

```bash
uv sync
uv run python codex_project_assigner.py --url ws://127.0.0.1:4500
```

Codex App Server が WebSocket で待ち受けている場合のコマンド例は `codex app-server --listen ws://127.0.0.1:4500` です。ただし、これは**新しいサーバーを起動するコマンド**です。すでに動いている App Server を再利用する場合は、その既存プロセスの endpoint を `--url` で指定してください。

Unix socket の待受先がある場合:

```bash
uv run python codex_project_assigner.py --unix-socket /path/to/codex-app-server.sock
```

認証付き WebSocket の場合は、トークンをコマンド引数に直接書かず、環境変数 `CODEX_APP_SERVER_TOKEN` に設定してください。非ローカル接続は `wss://` または SSH トンネルで保護してください。

## オプション

- `--lookback-seconds 300`: 起動前の直近 5 分に作成された未割り当て thread も復旧対象とする。より古い既存 thread は変更しない。
- `--reconcile-interval 60`: 60 秒ごとに `thread/list` を使い、切断中やイベント欠落の thread を補完する。
- `--request-timeout 20`: JSON-RPC のリクエストタイムアウト（秒）。
- `--log-level DEBUG`: 詳細ログ。

## 実装上の留意点

- 比較対象は `thread.cwd` と各 `project.roots[].path`。`/foo/../bar` や末尾スラッシュは正規化し、ディレクトリの親子関係による暗黙一致は行わない。シンボリックリンクを介した同じディレクトリは、**文字列上のルートが異なる限り別扱い**。
- 同じ `cwd` の複数 thread は `asyncio.Lock` で直列化。同一プロセス内での重複 project 作成を防ぐ。**複数の watcher プロセスを並行起動しない**こと。サーバー API に cwd の一意制約 / compare-and-set はないため、異なるプロセス間での完全な重複防止は保証されない。
- `project/create` には必須の `idempotencyKey` を指定。タイムアウト時の同一処理の再送には同じキーを使う。
- `thread/read` による事前確認を行うが、`thread/metadata/update` に条件付き更新（compare-and-set）はないため、別クライアントによる**同時変更を完全には防げない**。
- `ephemeral: true` の thread は永続的な project 割り当てができないため対象外。
- `project/changed` でキャッシュを無効化。`project/list` がページネーションされても全件を読み込む。
- Desktop アプリの独自 project 管理と、App Server の project metadata が完全には同期しないという公開バグ報告があります。App Server 側の `projectId` が変わっても Desktop サイドバーへ即時反映されない／後から変更が戻る可能性があります。

## テスト

```bash
uv run python -m unittest -v test_codex_project_assigner.py
```

単体・擬似 WebSocket 統合テスト 11 ケースを実施済み。実際の Codex App Server への接続テストは未実施です。

## プロトコル資料

- https://learn.chatgpt.com/docs/app-server
- https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/project.rs
- https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs
- https://github.com/openai/codex/issues/54482
