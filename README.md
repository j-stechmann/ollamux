# ollamux

*ollamux* — an **Olla**ma **mu**ltiple**x**er: a mux, naturally.
(Previously published as `omlx`.)

A key-rotating reverse proxy for the [Ollama Cloud API](https://ollama.com).
Give it several API keys and it serves them combined behind one local
endpoint: requests are spread across keys (round-robin, least-loaded first),
and when a key is rate-limited, rejected, or invalid, the request silently
retries on the next key before anything reaches your client.

```
ollama CLI ──┐
OpenAI SDK ──┼──> ollamux :11435 ──> https://ollama.com
  curl ──────┘        (key 1..N, per-key concurrency slots, auto-failover)
```

## Quickstart

```sh
# 1. Install
cargo install --path .

# 2. Add your keys (https://ollama.com/settings/keys)
mkdir -p ~/.config/ollamux
printf '%s\n' 'your-key-one...' 'your-key-two...' > ~/.config/ollamux/keys
chmod 600 ~/.config/ollamux/keys

# 3. Point any Ollama client at it
export OLLAMA_HOST=http://localhost:11435
ollama run gpt-oss:120b
```

## Install (distro packages)

| Source              | Install                                            |
| ------------------- | -------------------------------------------------- |
| AUR (source build)  | `paru -S ollamux`                                     |
| AUR (prebuilt)      | `paru -S ollamux-bin`                                 |
| Fedora/COPR         | `dnf copr enable j-stechmann/ollamux && dnf install ollamux` |
| Debian (from release assets) | `apt install ./ollamux_0.1.0-1_amd64.deb`    |
| Container           | `docker run -p 11435:11435 -e OLLAMUX_KEYS=… ghcr.io/j-stechmann/ollamux` |
| From source         | `cargo install --locked ollamux` (crates.io) or `cargo install --path .` |

Prebuilt static binaries (x86_64/aarch64 musl) are attached to each
GitHub release. Packager notes: [`packaging/README.md`](packaging/README.md).

OpenAI-compatible clients use the same proxy with a base URL:

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:11435/v1", api_key="unused")
```

## Keys file

One key per line in `~/.config/ollamux/keys` (respects `XDG_CONFIG_HOME`),
or set `OLLAMUX_KEYS` (newline/comma-separated) to skip the file entirely.
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
| `/_usage`      | Per-key Ollama Cloud usage JSON (`?refresh=1` forces a refresh, at most one fetch attempt per 5 s) |
| `/_health`     | `{"ok":…, "keys":…, "total_slots":…}`            |

Everything else answers `404` with a hint — this is **not** a local Ollama
server; it serves no models and `ollama list` against it shows the cloud
model list, not local models.

## Usage introspection

Ollama Cloud meters usage per account (fraction of your plan's cap, not
tokens) on an **undocumented** endpoint, `GET https://ollama.com/api/usage`
— the same one the ollama.com settings page uses. The session window rolls
over roughly every 5 hours, the weekly one every 7 days; there are no reset
timestamps upstream. Since that endpoint could change or disappear without
warning, `/_usage` treats any payload drift as a per-key error string,
never a crash. (Proxied `/api/usage` requests are NOT special: they go
through normal key rotation and reflect whichever key served them — use
`/_usage` for the per-account picture.)

```sh
curl -s localhost:11435/_usage | jq .
curl -s 'localhost:11435/_usage?refresh=1' | jq .   # force a refresh
```

Forced refreshes are rate-limited to at most one upstream fetch attempt
per 5 s (a `?refresh=1` inside that window serves the cached snapshot —
`stale` keeps reflecting the 60 s TTL, not this guard). The guard counts
*attempts*, not successes: while the upstream is failing (failed rounds
keep the last good snapshot), polling loops back off instead of fanning
out on every request.

`/_usage` fans out to the usage endpoint with every configured key in
parallel and answers with one row per key (suffixes only, never secrets):
fresh session/weekly fractions plus percents, top models, the 4-week
rolling cost, or a per-key error otherwise. Responses are cached for 60 s
(`updated`/`age_s`/`stale` fields tell you the age); `/_keys` embeds the
latest known usage per key from the same cache — it never triggers an
upstream call itself, and usage checks never touch key health (a 401 there
is reported, not treated as a dead key).

```json
{"updated":1756620000,"age_s":3,"stale":false,"keys":[
  {"index":0,"suffix":"1234","ok":true,"session":0.037,"weekly":0.007,
   "session_pct":3.7,"weekly_pct":0.7,
   "models":[{"name":"gpt-oss:120b","request_count":42}],"cost":"$1.23"}]}
```

## Quota-aware key selection

```sh
ollamux --usage-aware        # demote keys at/over 80% session usage
ollamux --usage-aware=90     # custom threshold (1–99)
# or: OLLAMUX_USAGE_AWARE=80 (the flag wins when both are set)
```

When enabled, ollamux polls the usage endpoint every 60 s and orders
candidate keys so that keys whose session usage is at/over the threshold
are served **last** — demoted, never excluded: an over-quota key still
takes requests when no fresh key has a free slot. Failed usage fetches
keep the previous snapshot; with the feature off, routing behaves exactly
as before.

## What failover means here

- **429** (rate limit): the key cools down (60 s, or the server's
  `Retry-After`, capped at 5 min) and the request retries on the next key.
- **401/403 Unauthorized** (invalid key): the key is marked dead until
  restart; the request retries on the next key.
- **5xx / network errors**: retry the next key; three consecutive failures
  put a key in cooldown. Successes reset the counter. (Merely *admitting* a
  request to a key never resets its strike counter — only confirmed
  upstream successes do.)
- Everything else (e.g. a 400 from a malformed request) is passed through
  untouched — that's your bug, not a key problem.
- Failover happens *before the first response byte*, so streaming responses
  (NDJSON and SSE) are never corrupted mid-flight by a key switch.

If every key is cooling down or dead, you get a clear JSON error instead of
a mysterious upstream one — see `/_keys` for per-key state.

## Runtime notes

- Logs: silent by default. `-v` adds a per-request stderr line
  (`retries=N`, `key=<suffix>`), key cooldown/death events, startup banner,
  shutdown notices, and upstream error snippets; fatal errors always print.
- Response headers include `X-Ollamux` (ollamux/version — every response,
  including relayed upstream errors, is attributable), `X-Ollamux-Key`
  (which key served it) and `X-Ollamux-Retries`.
- `SIGINT` (ctrl-c) drains in-flight requests for up to 5 s, then exits;
  press again to force-quit.
- Request bodies are buffered up to 16 MiB (needed for replay across
  failover); larger bodies get `413`.
- Binds `127.0.0.1:11435` by default (`--addr` to change). The default port
  is *not* 11434 so it can run alongside a real local Ollama.

## Limitations (on purpose)

- Usage introspection is read-only reporting; there is no token accounting
  or historical dashboard — `/_usage` mirrors what ollama.com exposes per
  account.
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
cargo test          # unit + integration (hermetic: local upstream)
cargo test --features net   # also hits real ollama.com lightly
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## Reliability notes

- Invalid/concurrency counts in the keys file are startup errors, not
  silently defaulted: `KEY:99999999999` refuses to start, duplicate key
  lines are rejected (they would double-count slots), and a keys file
  with only comments refuses to start.
- Non-ASCII key lines are fine (suffixes use character boundaries).
- Upstream redirects are not followed (the proxy classifies or relays
  them verbatim); query strings are forwarded to upstream untouched.
- Relayed upstream error bodies are byte-exact (no truncation).
- SIGINT shutdown is signal-mask based: the first ctrl-c starts the
  drain, the second force-quits (exit code 130).