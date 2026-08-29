# omlx

A key-rotating reverse proxy for the [Ollama Cloud API](https://ollama.com).
Give it several API keys and it serves them combined behind one local
endpoint: requests are spread across keys (round-robin, least-loaded first),
and when a key is rate-limited, rejected, or invalid, the request silently
retries on the next key before anything reaches your client.

```
ollama CLI ──┐
OpenAI SDK ──┼──> omlx :11435 ──> https://ollama.com
  curl ──────┘        (key 1..N, per-key concurrency slots, auto-failover)
```

## Quickstart

```sh
# 1. Install
cargo install --path .

# 2. Add your keys (https://ollama.com/settings/keys)
mkdir -p ~/.config/omlx
printf '%s\n' 'your-key-one...' 'your-key-two...' > ~/.config/omlx/keys
chmod 600 ~/.config/omlx/keys

# 3. Point any Ollama client at it
export OLLAMA_HOST=http://localhost:11435
ollama run gpt-oss:120b
```

OpenAI-compatible clients use the same proxy with a base URL:

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:11435/v1", api_key="unused")
```

## Keys file

One key per line in `~/.config/omlx/keys` (respects `XDG_CONFIG_HOME`),
or set `OMLX_KEYS` (newline/comma-separated) to skip the file entirely.
Blank lines and `#` comments are allowed.

Optionally set a per-key concurrency limit with a `:N` suffix — the number of
cloud models that account may run at once (free=1, pro=3, max=10):

```
your-free-key...:1
your-pro-key...:3           # default when no suffix is given
your-max-key...:10
```

The proxy keeps at most N requests in flight per key (matching Ollama
Cloud's per-plan concurrency limits) and queues the rest briefly rather
than hammering upstream. Requests that wait too long get an honest `429`.

## Endpoints

| Path            | Behavior                                        |
| --------------- | ----------------------------------------------- |
| `/api/*`       | Proxied to `https://ollama.com` with rotation    |
| `/v1/*`        | Proxied (OpenAI-compatible surface) with rotation|
| `/_keys`       | Per-key health JSON (suffixes only, no secrets)  |
| `/_health`     | `{"ok":…, "keys":…, "total_slots":…}`            |

Everything else answers `404` with a hint — this is **not** a local Ollama
server; it serves no models and `ollama list` against it shows the cloud
model list, not local models.

## What failover means here

- **429** (rate limit): the key cools down (60 s, or the server's
  `Retry-After`, capped at 5 min) and the request retries on the next key.
- **401/403 Unauthorized** (invalid key): the key is marked dead until
  restart; the request retries on the next key.
- **5xx / network errors**: retry the next key; three consecutive failures
  put a key in cooldown. Successes reset the counter.
- Everything else (e.g. a 400 from a malformed request) is passed through
  untouched — that's your bug, not a key problem.
- Failover happens *before the first response byte*, so streaming responses
  (NDJSON and SSE) are never corrupted mid-flight by a key switch.

If every key is cooling down or dead, you get a clear JSON error instead of
a mysterious upstream one — see `/_keys` for per-key state.

## Runtime notes

- Logs: one stderr line per request (`retries=N`, `key=<suffix>`);
  `-v` adds upstream error snippets.
- Response headers include `X-Omlx-Key` (which key served it) and
  `X-Omlx-Retries`.
- `SIGINT` (ctrl-c) drains in-flight requests for up to 5 s, then exits;
  press again to force-quit.
- Request bodies are buffered up to 16 MiB (needed for replay across
  failover); larger bodies get `413`.
- Binds `127.0.0.1:11435` by default (`--addr` to change). The default port
  is *not* 11434 so it can run alongside a real local Ollama.

## Limitations (on purpose)

- No usage dashboards, token accounting, or provisioning — it's a proxy.
- No request rewriting: models must exist on ollama.com.
- A dead key stays dead until restart (`/_keys` shows why). Restarting is
  cheap: it's stateless.
- Localhost trust model: anything that can reach the port can use your
  quota. Don't expose it beyond loopback.

## A note on accounts

Ollama's terms currently describe one account per person; this tool exists
to pool keys you legitimately hold (e.g. personal + work seats). Don't use
it to evade per-user limits you're not entitled to.

## Development

```sh
cargo test          # unit + integration (integration hits real ollama.com lightly)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```