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
| `/_usage`      | Per-key usage JSON + pool-capacity aggregate (`?refresh=1` forces a refresh, at most one fetch attempt per 5 s) |
| `/_health`     | `{"ok":…, "keys":…, "total_slots":…}`            |

Everything else answers `404` with a hint — this is **not** a local Ollama
server; it serves no models and `ollama list` against it shows the cloud
model list, not local models.

The full API surface is described in [`openapi.yaml`](openapi.yaml)
(OpenAPI 3.0): the proxied Ollama/OpenAI paths, the `/_keys`, `/_usage`,
`/_health` introspection endpoints, and ollamux's response headers and
error envelope.

## Usage introspection

Ollama Cloud publishes plan usage on the **documented** endpoint
`GET https://ollama.com/api/balance` (docs.ollama.com/api/balance), one
request per key. Legacy plans (session/weekly limits) answer with what
*remains* of each window's allowance, reset instants included:

```json
{"included":{"session":{"remaining_percent":75,"resets_at":"2026-10-01T07:00:00Z"},
             "weekly": {"remaining_percent":40,"resets_at":"2026-10-05T00:00:00Z"}},
 "purchased":{"balance_usd":0}}
```

Credit plans answer with USD amounts instead (`included.balance_usd`,
`included.allowance_usd`, `included.period`, `purchased.balance_usd`).
Rate limit: 10 requests per minute per user, shared across API keys —
the docs recommend polling once per minute, which ollamux's passive
60 s TTL respects for pools of up to ~10 keys. Larger pools fan out
one GET per key in parallel, so a single round can burst over the
shared per-user limit; affected keys report `rate limited (upstream
429)` and back off. `/_usage` treats any payload drift as a per-key
error string, never a crash. (Proxied `/api/usage` requests are NOT
special: they go through normal key rotation and reflect whichever key
served them — use `/_usage` for the per-account picture.)

```sh
curl -s localhost:11435/_usage | jq .
curl -s 'localhost:11435/_usage?refresh=1' | jq .   # force a refresh
```

Forced refreshes are rate-limited to at most one upstream fetch attempt
per 5 s (a `?refresh=1` inside that window serves the cached snapshot —
`stale` keeps reflecting the 60 s TTL, not this guard). Note this guard
is independent of the TTL: a forced-refresh loop fetches up to 12
times/min per key, over the upstream rate limit even for a single key.
The guard counts
*attempts*, not successes: while the upstream is failing (failed rounds
keep the last good snapshot), polling loops back off instead of fanning
out on every request.

`/_usage` fans out to the balance endpoint with every configured key in
parallel (one GET per key) and answers with one row per key (suffixes
only, never secrets): on legacy plans the used fraction of each window —
`session`/`weekly` in 0.0–1.0 (upstream's remaining percent, inverted),
one-decimal `*_pct` mirrors, and `session_resets_at`/`weekly_resets_at`
for countdowns — or, on credit-plan keys, `included_usd`/`allowance_usd`
and `purchased_usd` with no percent windows, or a per-key error
otherwise. Each row also carries the key's plan `tier`, inferred from
its per-key concurrency (`KEY:N` — free=1, pro=3, max=10; more
concurrency than normal is the next tier up). Responses are cached for
60 s (`updated`/`age_s`/`stale` fields tell you the age); `/_keys` embeds
the latest known usage per key from the same cache — it never triggers an
upstream call itself, and usage checks never touch key health (a 401
there is reported, not treated as a dead key).

The envelope's `aggregate` is the capacity-weighted mean of every
legacy-plan reporting key's usage fraction, weighted by its tier's cap —
free ×1, pro ×50, max ×250 (pro has 50× free's usage cap, max 5× pro's) —
so it reads as *the fraction of the pool's combined capacity in use* and
stays in 0.0–1.0 like the per-key numbers: a single-key pool reproduces
that key's own fraction, and applications built for the per-key range
need no adjustment. All keys contribute, including ones currently on
cooldown (usage fetching is health-blind); a dead key (401/403) is
expected to fail its own usage fetch the same way and contributes
nothing (its weight is excluded from the denominator, so it never
dilutes the number). Windows nobody reported are `null`, never 0; when
every reporting key of a window carries the same `resets_at`, the
aggregate publishes it (`session_resets_at`/`weekly_resets_at`) so
consumers get one honest reset instant for the pool. Credit-plan rows
carry no window fractions and sit out the aggregate (their unit is USD,
not a fraction of an allowance).

```json
{"updated":1756620000,"age_s":3,"stale":false,
 "session_window":"plan session window (reset timestamps upstream; poll courtesy 60 s)",
 "aggregate":{"session":0.807,"weekly":0.418,
              "session_resets_at":"2026-10-09T00:00:00Z",
              "unit":"pool capacity fraction"},
 "keys":[
   {"index":0,"suffix":"1234","ok":true,"tier":"free",
    "session":0.037,"weekly":0.007,"session_pct":3.7,"weekly_pct":0.7,
    "session_resets_at":"2026-10-09T00:00:00Z",
    "weekly_resets_at":"2026-10-12T00:00:00Z"},
   {"index":1,"suffix":"5678","ok":true,"tier":"max",
    "session":0.81,"weekly":0.42,"session_pct":81.0,"weekly_pct":42.0,
    "session_resets_at":"2026-10-09T00:00:00Z",
    "weekly_resets_at":"2026-10-12T00:00:00Z"}]}
```

## Quota-aware key selection

```sh
ollamux --usage-aware        # demote keys at/over 80% session usage
ollamux --usage-aware=90     # custom threshold (1–99)
# or: OLLAMUX_USAGE_AWARE=80 (the flag wins when both are set)
```

When enabled, ollamux polls the balance endpoint every 60 s and orders
candidate keys so that keys whose session usage is at/over the threshold
are served **last** — demoted, never excluded: an over-quota key still
takes requests when no fresh key has a free slot. Failed usage fetches
keep the previous snapshot; with the feature off, routing behaves exactly
as before.

Credit-plan keys are exempt from demotion: their balance responses carry
USD amounts, not a session fraction, so there is no percent to compare
against the threshold — under `--usage-aware` they are ordered like
unmeasured keys (demote only ever applies to legacy-plan percent data).
The demotion signal is the *session* window alone: a key whose weekly
window is exhausted but whose session window is under the threshold is
never demoted.

## Prompt-cache affinity (on by default)

Ollama Cloud caches prompt prefixes server-side, per account (i.e. per
API key). ollamux's normal least-loaded routing would send successive
requests of one conversation to different keys and re-ingest the same
prefix every turn. With affinity, ollamux computes a conversation
identity from the request body (`model` + the leading system messages +
first user message for chat; `model` + first 8 KiB of the prompt for
`/api/generate` and `/v1/completions`) and pins it to the key that last
served it — as long as that key is healthy, has a free slot, and is not
demoted by `--usage-aware`. When the pinned key cannot serve right now,
the request is routed normally (no waiting on a busy key); a cache miss
just means the next success re-pins.

- Disable with `--no-affinity` or `OLLAMUX_NO_AFFINITY=1`.
- Responses served by a pool key carry `X-Ollamux-Affinity: hit|miss|off`
  (`off` = disabled or the body gave no identity; proxy-generated
  failures where no key was selected — admission rejects, exhausted
  failover — omit the header). `hit` means a pin existed **and was
  usable** (healthy, under quota, free slot) at admission; `miss` covers
  first requests, evictions, and pins that fell through.
- Helps append-only conversations (the common case: chat turns only
  extend the messages array). Clients that rewrite the system prompt or
  trim history mid-conversation simply get the old rotation behavior.
- The pin map holds 4096 conversations (LRU-evicted, in-memory only).

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
  (which key served it), `X-Ollamux-Retries` and `X-Ollamux-Affinity`
  (prompt-cache affinity: `hit`/`miss`/`off`; present only when a key
  served the request).
- `SIGINT` (ctrl-c) drains in-flight requests for up to 5 s, then exits;
  press again to force-quit.
- Request bodies are buffered up to 16 MiB (needed for replay across
  failover); larger bodies get `413`.
- Binds `127.0.0.1:11435` by default (`--addr` to change). The default port
  is *not* 11434 so it can run alongside a real local Ollama.

## Limitations (on purpose)

- Usage introspection is read-only reporting; there is no token accounting
  or historical dashboard — `/_usage` mirrors what ollama.com exposes per
  account (the only derived figure is the capacity-weighted pool
  `aggregate`, which ollama.com itself does not publish).
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

### Branching model (git flow)

- `develop` is the integration branch, `master` holds release tags.
  Both are protected: changes land via PR only, gated on the `check`
  and `packaging-lint` CI checks (enforced for admins too).
- Feature branches (`feat/...`, `fix/...`, `release/vX.Y.Z`) PR into
  `develop`.
- Releases PR `develop` into `master` and tag `vX.Y.Z` on `master`;
  the tag push triggers the release pipeline
  ([packaging/README.md](packaging/README.md) has the full checklist).

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