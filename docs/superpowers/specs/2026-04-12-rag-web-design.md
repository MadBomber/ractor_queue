# RAG Web App — Design Spec

**File:** `examples/04_rag_web.rb`
**Date:** 2026-04-12
**Status:** Approved

---

## Purpose

Replace the `ratatui_ruby` TUI in `examples/03_rag_pipeline.rb` with a Sinatra web app served
by Falcon (the Async-native Rack server). The same Ractor + Async + RactorQueue concurrency
architecture is preserved; the only change is the consumer of `progress_queue` — a WebSocket
broadcast fiber replaces the TUI render loop.

Shows: Falcon + Sinatra, async-websocket, Ractors, Async threads, and RactorQueue all working
together. RactorQueue is the universal message bus bridging every tier.

`examples/03_rag_pipeline.rb` (TUI version) is kept as-is alongside this new example.

---

## CLI Interface

```bash
bundle exec ruby examples/04_rag_web.rb --db ~/.ractor_rag/index.db --port 3000
```

- `--db PATH` — path to persist the sqlite-vec database (default: temp file, printed as warning)
- `--port N` — HTTP port (default: 3000)
- Server prints startup URL to the terminal on launch
- Models are pre-loaded serially at startup before the server begins accepting requests
- Pipeline does **not** start automatically — user initiates via the browser

---

## File Structure

```
examples/
  03_rag_pipeline.rb          # existing TUI version — unchanged
  04_rag_web.rb               # Falcon/Sinatra server (single Ruby file)
  04_rag_web_public/
    index.html                # single-page app — HTML + inline JS, no build step
```

Two gems added to the `development, test` group in `Gemfile`:
- `falcon` — Async-native Rack server (replaces WEBrick/Puma for this example)
- `async-websocket` — WebSocket support inside an Async reactor

---

## Architecture

### Concurrency Tiers

```
INGEST PIPELINE (starts on POST /index)
  Reader Ractor ──doc_queue──▶ Chunker Ractors×N ──chunk_queue──▶ Embedder Async Threads×N
                                                                        ──embed_queue──▶ Writer Async Thread ──▶ SQLite

QUERY PIPELINE (on WS message {type:"query"})
  Falcon WS handler ──query_jobs──▶ QueryEmbedder Async Thread ──query_results──▶ Falcon WS handler ──▶ browser

PROGRESS BROADCAST
  Reader/Chunkers & Embedders/Writer ──progress_queue──▶ broadcast Async fiber (100ms tick) ──▶ all WS clients
```

### Worker Counts

```ruby
N_EMBEDDERS = (Etc.nprocessors / 2).clamp(2, 6)
N_CHUNKERS  = 2
```

### Ractor Tier (pure Ruby, true parallelism)
- **Reader** — `Dir.glob` + `File.read`; Ractor-safe
- **Chunkers ×N** — `ChunkerRuby::RecursiveCharacter`; pure Ruby, Ractor-safe

### Async Thread Tier (gem-dependent, GVL-releasing)
- **Embedders ×N** — Informers/ONNX; each Thread has its own Async reactor and pre-loaded model
- **Writer** — sqlite3; Async reactor for `async_pop` while waiting for embedded chunks
- **QueryEmbedder** — Informers/ONNX + sqlite3 read handle; Async reactor

### Falcon (Async reactor, main thread)
Falcon runs the Sinatra app on an Async reactor. All WebSocket connections live as Async fibers
within that reactor. The broadcast fiber also runs here, draining `progress_queue` on a 100ms
interval and writing to every connected socket.

---

## Queue Capacities

| Queue              | Payload          | Capacity | Notes                                    |
|--------------------|------------------|----------|------------------------------------------|
| `doc_queue`        | `RawDocument`    | 64       | Reader is fast; small buffer is fine     |
| `chunk_queue`      | `RawChunk`       | 256      | Chunkers outpace Embedders; absorbs burst |
| `embed_queue`      | `EmbeddedChunk`  | 128      | Embedders are the bottleneck             |
| `progress_queue`   | `ProgressEvent`  | 512      | Fire-and-forget; large so nothing blocks |
| `query_jobs`       | `QueryJob`       | 8        | Single user, no bursting                 |
| `query_results`    | `QueryResult`    | 8        | Broadcast fiber drains each tick         |

---

## Data Types

Identical to `03_rag_pipeline.rb`. All Struct instances are `Ractor.make_shareable`'d before
pushing. `:shutdown` (frozen symbol) is the poison pill.

```ruby
RawDocument   = Struct.new(:id, :path, :text)
RawChunk      = Struct.new(:doc_id, :chunk_index, :text, :source_path)
EmbeddedChunk = Struct.new(:doc_id, :chunk_index, :text, :vector_blob, :source_path)
ProgressEvent = Struct.new(:stage, :count, :detail)
QueryJob      = Struct.new(:id, :text)
QueryResult   = Struct.new(:query_id, :query_text, :hits)
Hit           = Struct.new(:text, :score)
```

---

## HTTP Routes

| Method | Path              | Description                                                    |
|--------|-------------------|----------------------------------------------------------------|
| GET    | `/`               | Serves `04_rag_web_public/index.html`                         |
| GET    | `/browse`         | JSON directory listing for `?path=` param                      |
| POST   | `/index`          | Start ingest pipeline; no-op if already running; returns JSON  |
| GET    | `/status`         | JSON snapshot of current pipeline state                        |
| GET    | `/ws`             | WebSocket upgrade (progress stream + query interface)          |

### `GET /browse` response

```json
{
  "path": "/Users/dewayne/docs",
  "parent": "/Users/dewayne",
  "dirs":  ["obsidian_vault", "projects"],
  "files": ["notes.md", "readme.txt"]
}
```

Only `.txt`, `.md`, and `.rb` files are listed. Dotfiles and system directories are hidden.

### `POST /index` response

```json
{"status": "started", "dir": "/Users/dewayne/docs"}
{"status": "already_running", "dir": "/Users/dewayne/docs"}
```

### `GET /status` response

```json
{
  "running":   true,
  "dir":       "/Users/dewayne/docs",
  "db":        "/Users/dewayne/.ractor_rag/index.db",
  "docs":      42,
  "total":     147,
  "chunks":    318,
  "embedded":  289,
  "stored":    280,
  "done":      false
}
```

---

## WebSocket Protocol

One WebSocket connection per client handles both directions.
All messages are JSON strings.

### Server → Client

```json
{"type":"progress","stage":"total","count":147,"detail":"found 147 files in /Users/dewayne/docs"}
{"type":"progress","stage":"doc","count":42,"detail":"notes.md"}
{"type":"progress","stage":"chunk","count":318,"detail":"chunked notes.md (12)"}
{"type":"progress","stage":"embed","count":289,"detail":"chunk 289 → 384-dim (23ms)"}
{"type":"progress","stage":"store","count":280,"detail":"batch #5 committed (50 rows)"}
{"type":"progress","stage":"done","count":1420,"detail":"index complete — 1420 chunks stored"}
{"type":"result","query_id":1,"query_text":"What is VSM?","hits":[{"text":"...","score":0.94}]}
{"type":"error","message":"Ractor crashed: FrozenError ..."}
{"type":"status", ...}  # current pipeline snapshot on WS connect
```

### Client → Server

```json
{"type":"query","text":"What is the Viable Systems Model?"}
```

On connect, the server immediately sends a `{"type":"status", ...}` snapshot so the page
renders current state even if the user connects mid-run.

---

## Broadcast Fiber

A single Async fiber runs inside the Falcon reactor and broadcasts progress to all connected
WebSocket clients:

```ruby
# Pseudo-code — runs inside Async { } on Falcon's reactor
Async do
  loop do
    # Drain up to SPIN_DRAIN events per tick
    SPIN_DRAIN.times do
      ev = progress_queue.try_pop
      break if ev.equal?(RactorQueue::EMPTY)
      msg = {type: "progress", stage: ev.stage, count: ev.count, detail: ev.detail}.to_json
      ws_clients.each { |ws| ws.write(msg) rescue nil }
      # Also drain query_results
    end
    sleep 0.1  # yields to Async scheduler; ~10 ticks/sec
  end
end
```

`ws_clients` is an Array managed inside the Falcon Async reactor (single-threaded access, no
Mutex needed because Falcon serializes all fiber execution within the reactor).

---

## Directory Browser (server-side)

`GET /browse?path=/some/dir` returns a JSON listing of that directory. The browser JS
maintains a `currentPath` variable and renders the listing as a clickable tree. Clicking a
directory calls `/browse` again with the new path. Clicking "Select" sets `currentPath` as
the target directory for `POST /index`.

Security: path is validated with `File.realpath` and must be an existing directory. No path
traversal is possible (the route returns 400 on `Errno::ENOENT` or if path is not a directory).

---

## Frontend (index.html)

Single HTML file with inline `<style>` and `<script>`. No external dependencies; no build step.

### Layout (single-column, top to bottom)

1. **Header** — "RactorQueue RAG Demo" + WebSocket connection status badge
2. **Directory row** — current path text field (read-only) + "Browse" button + "▶ Index" button
3. **Progress bar** — gradient fill 0–100% based on `stored / total`
4. **Stats row** — four cards: Docs · Chunks · Embedded · Stored
5. **Activity log** — single line, last progress event detail
6. **Query row** — text input + "Ask" button (disabled until `done == true`)
7. **Results panel** — renders `hits` from last `{"type":"result"}` message

### WebSocket lifecycle (JS)

```
onopen  → send nothing; wait for status snapshot from server
onmessage → dispatch on msg.type: "progress" | "result" | "error" | "status"
onclose → show "disconnected" badge; retry after 3s
```

Query is submitted by pressing Enter or clicking "Ask". Sends
`{"type":"query","text":"..."}` over the open WebSocket. Results render when the
`{"type":"result"}` message arrives.

---

## Model Pre-loading

Identical to `03_rag_pipeline.rb`: all `N_EMBEDDERS + 1` model instances are loaded serially
in the main thread at server startup, before Falcon begins accepting connections. Progress
printed to the launching terminal. Workers receive pre-loaded models; no ONNX init happens
inside the Async reactor or during an active WebSocket session.

---

## Error Handling

- Ractor crashes logged to `/tmp/ractor_errors.log` (same as 03)
- Crash also broadcast as `{"type":"error","message":"..."}` to all WS clients
- Pipeline running state is tracked via an `@pipeline_running` flag (set true on start, false when Writer pushes `:done` or on Ractor crash)
- `POST /index` while `@pipeline_running` is true returns `{"status":"already_running"}` — no restart
- If pipeline crashes mid-run, `@pipeline_running` is set to false by the Ractor monitor; subsequent `POST /index` restarts from scratch with fresh queues and workers
- `/browse` returns `{"error":"not a directory"}` with HTTP 400 for invalid paths
- WebSocket write errors are rescued and the client silently removed from `ws_clients`

---

## Shutdown

`Ctrl-C` stops Falcon. Ractors are OS threads and die with the process. No graceful drain is
attempted on shutdown — this is a demo, not a production service.

---

## Testing

No automated tests for the web app itself (Sinatra/Falcon integration is out of scope for the
gem's test suite). Manual test checklist:

- [ ] Server starts, pre-loads models, prints URL
- [ ] Browser loads `index.html`; WS status badge shows "connected"
- [ ] `/browse` navigates filesystem; "Select" populates path field
- [ ] `POST /index` starts pipeline; progress stats update live
- [ ] Progress bar reaches 100%; "done" status shown
- [ ] Query submitted while indexing is disabled (button greyed out)
- [ ] Query submitted after done; results appear within a few seconds
- [ ] Second browser tab connects mid-run; receives status snapshot immediately
- [ ] `/browse` with invalid path returns 400
- [ ] `POST /index` while running returns `already_running`
