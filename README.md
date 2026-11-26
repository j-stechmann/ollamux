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
#    Format: KEY[:N]@ACCOUNT — the account label is required.
mkdir -p ~/.config/ollamux
printf '%s\n' 'your-key...:3@you' > ~/.config/ollamux/keys
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
| Container           | `docker run -p 11435:11435 -e OLLAMUX_KEYS='key:3@you' ghcr.io/j-stechmann/ollamux` |
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
Blank lines and `#` comments (whole-line or trailing) are allowed.

The format is `KEY[:N]@ACCOUNT`. **The account label is required** (a
missing label is a startup error): balance and usage are account-scoped
upstream, so ollamux needs the label to fetch each account once and to
attribute the numbers correctly. Labels are 1–64 characters of
`A-Z a-z 0-9 . _ -`. Keys sharing a label are treated as belonging to one
ollama.com account.

Optionally set a concurrency limit with a `:N` suffix — the number of
cloud models the account may run at once (free=1, pro=3, max=10; default
3):

```
your-free-key...:1@free-acct
your-pro-key...:3@pro-acct      # 3 is the default when no suffix is given
your-max-key...:10@max-acct
```

A group of friends can pool separate accounts; each account gets its own
usage rows and model statistics:

```
alice-key-1...:1@alice
alice-key-2...:1@alice
bob-key...:10@bob
```

The proxy keeps at most N requests in flight per key and queues the rest
briefly rather than hammering upstream. Requests that wait too long get
an honest `429`. Note that slots are still counted **per key**, while
Ollama Cloud's concurrency limit is **per account**: two `:3` keys under
one label allow up to 6 in flight, and ollamux warns about that at
startup. Use one key per account for a strict plan cap.

## Endpoints

| Path            | Behavior                                        |
| --------------- | ----------------------------------------------- |
| `/api/*`       | Proxied to `https://ollama.com` with rotation    |
| `/v1/*`        | Proxied (OpenAI-compatible surface) with rotation|
| `/_keys`       | Per-key health JSON (suffixes/account labels only, no secrets) |
| `/_usage`      | Per-account + per-key balance JSON and pool aggregate; add `?range=24h\|7d\|30d` for per-account model/time statistics (`group_by=model` optional; `?refresh=1` forces a refresh, at most one fetch attempt per 5 s) |
| `/_stats`      | Local per-request counters (requests, upstream-reported tokens, duration, retries) by key/account/model/endpoint and UTC hour/day bucket; `?range=24h\|7d\|30d` (default `24h`) |
| `/_health`     | `{"ok":…, "keys":…, "total_slots":…}`            |

Everything else answers `404` with a hint — this is **not** a local Ollama
server; it serves no models and `ollama list` against it shows the cloud
model list, not local models.

The full API surface is described in [`openapi.yaml`](openapi.yaml)
(OpenAPI 3.0): the proxied Ollama/OpenAI paths, the `/_keys`, `/_usage`,
`/_stats`, `/_health` introspection endpoints, and ollamux's response
headers and error envelope.

## Usage introspection

Ollama Cloud publishes plan usage on the **documented** endpoint
`GET https://ollama.com/api/balance` (docs.ollama.com/api/balance), one
request per **account** (not per key). Legacy plans (session/weekly
limits) answer with what *remains* of each window's allowance, reset
instants included:

```json
{"included":{"session":{"remaining_percent":75,"resets_at":"2026-10-01T07:00:00Z"},
             "weekly": {"remaining_percent":40,"resets_at":"2026-10-05T00:00:00Z"}},
 "purchased":{"balance_usd":0}}
```

Credit plans answer with USD amounts instead (`included.balance_usd`,
`included.allowance_usd`, `included.period`, `purchased.balance_usd`).
Rate limit: 10 requests per minute **per user**, shared across that
user's API keys and devices — the docs recommend polling once per minute,
which ollamux's passive 60 s TTL respects for pools of up to ~10
accounts. A 429'd account keeps its last good row (an `as_of` timestamp
marks when that data was fetched) and sits out the next round — or the
server's `Retry-After`, capped at 5 min — so large pools rotate the
fetch budget across accounts instead of losing data. Transient failures
(429, 5xx, network blips) carry the last good row; persistent ones
(unauthorized, endpoint gone, payload drift) surface as per-account
error strings, never a crash.

```sh
curl -s localhost:11435/_usage | jq .
curl -s 'localhost:11435/_usage?refresh=1' | jq .              # force a refresh
curl -s 'localhost:11435/_usage?range=7d' | jq .               # + model/time stats
curl -s 'localhost:11435/_usage?range=30d&group_by=model' | jq .
```

Forced refreshes are rate-limited to at most one upstream fetch attempt
per 5 s (a `?refresh=1` inside that window serves the cached snapshot —
`stale` keeps reflecting the 60 s TTL, not this guard). The guard counts
*attempts*, not successes: while the upstream is failing (failed rounds
keep the last good snapshot), polling loops back off instead of fanning
out on every request.

`/_usage` fans out to the balance endpoint with every distinct
`@account` (one GET per account, in parallel; accounts in 429 backoff sit
the round out). It answers with an `accounts` array plus one row per key
(suffixes and account labels only, never secrets): on legacy plans the
used fraction of each window — `session`/`weekly` in 0.0–1.0 (upstream's
remaining percent, inverted), one-decimal `*_pct` mirrors, and
`session_resets_at`/`weekly_resets_at` for countdowns — or, on
credit-plan keys, `included_usd`/`allowance_usd` and `purchased_usd` with
no percent windows, or an error otherwise. Rows carried forward from a
previous round carry `as_of`, the unix time of their original fetch —
absent means fresh this round. Each key row also carries its `account`
label and the account's plan `tier`, inferred from the concurrency
suffix (`:N` — free=1, pro=3, max=10; more concurrency than normal is the
next tier up). Responses are cached for 60 s (`updated`/`age_s`/`stale`
fields tell you the age); `/_keys` embeds the latest known usage per key
from the same cache — it never triggers an upstream call itself, and
usage checks never touch key health (a 401 there is reported, not treated
as a dead key).

The envelope's `aggregate` is the capacity-weighted mean of every
legacy-plan reporting **account's** usage fraction, weighted by its
tier's cap — free ×1, pro ×50, max ×250 (pro has 50× free's usage cap,
max 5× pro's) — so it reads as *the fraction of the pool's combined
capacity in use* and stays in 0.0–1.0 like the per-key numbers. Weighting
is per account: two keys under one `@label` contribute once, never
double-counted. All accounts contribute, including ones currently on
cooldown (usage fetching is health-blind) and ones whose latest fetch
failed transiently (carried rows keep contributing their last known good
fraction); an unauthorized account contributes nothing (its weight is
excluded from the denominator). Windows nobody reported are `null`, never
0; when every reporting account of a window carries the same `resets_at`,
the aggregate publishes it. Credit-plan rows carry no window fractions
and sit out the aggregate (their unit is USD, not a fraction of an
allowance).

```json
{"updated":1756620000,"age_s":3,"stale":false,
 "session_window":"plan session window (reset timestamps upstream; poll courtesy 60 s)",
 "aggregate":{"session":0.807,"weekly":0.418,
              "session_resets_at":"2026-10-09T00:00:00Z",
              "unit":"pool capacity fraction"},
 "accounts":[
   {"account":"alice","keys":2,"ok":true,"tier":"pro",
    "session":0.037,"weekly":0.007,"session_pct":3.7,"weekly_pct":0.7},
   {"account":"bob","keys":1,"ok":true,"tier":"max",
    "session":0.81,"weekly":0.42,"session_pct":81.0,"weekly_pct":42.0,
    "as_of":1756619955}],
 "keys":[
   {"index":0,"suffix":"1234","account":"alice","ok":true,"tier":"pro",
    "session":0.037,"weekly":0.007,"session_pct":3.7,"weekly_pct":0.7,
    "session_resets_at":"2026-10-09T00:00:00Z",
    "weekly_resets_at":"2026-10-12T00:00:00Z"},
   {"index":1,"suffix":"5678","account":"alice","ok":true,"tier":"pro",
    "session":0.037,"weekly":0.007,"session_pct":3.7,"weekly_pct":0.7},
   {"index":2,"suffix":"9012","account":"bob","ok":true,"tier":"max",
    "session":0.81,"weekly":0.42,"session_pct":81.0,"weekly_pct":42.0,
    "session_resets_at":"2026-10-09T00:00:00Z",
    "weekly_resets_at":"2026-10-12T00:00:00Z",
    "as_of":1756619955}]}
```

(In the example, bob's latest fetch failed transiently — its data was
fetched 45 s before the snapshot completed, hence `as_of`; alice's rows
are fresh and carry no `as_of`.)

## Model and time statistics

Add `?range=24h`, `?range=7d` or `?range=30d` to `/_usage` and ollamux
also calls the documented cloud usage endpoint
`GET https://ollama.com/api/usage` (docs.ollama.com/api/cloud-usage),
once per account, with `group_by=model`. The envelope gains a `models`
section: per-account `totals` and UTC `buckets` carrying request counts,
tokens and USD cost, plus a per-model `groups` breakdown. `24h` is hourly
(25 buckets), `7d`/`30d` are daily; the current bucket has
`partial: true`.

```json
{"range":"7d","granularity":"day","group_by":"model",
 "from":"2026-09-24T00:00:00Z","until":"2026-10-01T02:30:00Z",
 "updated":1756620000,"age_s":3,"stale":false,
 "accounts":[
   {"account":"alice","keys":2,"ok":true,
    "totals":{"request_count":16,"usage_usd":0.0268,
              "input_tokens":116000,"cached_input_tokens":60000,"output_tokens":15000,
              "groups":[{"model":"gpt-oss:120b","request_count":16,
                         "usage_usd":0.0268,"input_tokens":116000,
                         "cached_input_tokens":60000,"output_tokens":15000}]},
    "buckets":[{"from":"2026-09-30T00:00:00Z","until":"2026-10-01T00:00:00Z",
                "groups":[{"model":"gpt-oss:120b","request_count":8,
                           "usage_usd":0.0134,"input_tokens":58000,
                           "cached_input_tokens":30000,"output_tokens":7500}]}]}],
 "pool":{"totals":{"request_count":16,"usage_usd":0.0268}}}
```

Things to know:

- The breakdown is **per account**, because the endpoint is
  account-scoped: two keys under one `@label` cannot be separated
  upstream.
- Up to 10 models appear in `groups` (ranked by cost, or request count on
  legacy plans); the rest are combined into `remainder`. `totals` always
  covers everything.
- There is **no per-request log** — the finest time resolution is one
  hour, and prompts/completions are never exposed. Custom ranges and
  per-API-key/per-member breakdowns are listed as "coming soon" by
  Ollama.
- "New usage may take time to appear" (upstream's own caveat). The
  `updated`/`age_s`/`stale` fields describe ollamux's cache, not
  reporting lag.
- Members of one Ollama **team** share an account and balance, so they
  cannot be separated by the documented API; separate accounts are
  required for per-person statistics.
- `group_by=model` is the only supported grouping; anything else is a
  `400`. The stats cache is per `(account, range)` with the same 60 s TTL,
  5 s forced-refresh guard and 429 carry/backoff behavior as balance.
- Proxied `/api/usage` requests are **not** special: they go through
  normal key rotation and reflect whichever key served them — use
  `/_usage` for the pooled, per-account picture.

## Local token statistics

Add `?range=24h`, `?range=7d` or `?range=30d` to `GET /_stats` (default
`24h`) for counters measured from the traffic that actually passed
through ollamux. Unlike `/_usage` (which mirrors account-scoped numbers
from ollama.com), this is per **key**, per **account**, per **model**, per
**endpoint**, and in UTC hour/day buckets. `24h` renders 25 hourly
buckets (the current partial hour plus 24 previous); `7d`/`30d` render
daily buckets.

Token counts come from the upstream response body —
`prompt_eval_count`/`eval_count`/`prompt_eval_cached_count` on Ollama's
native API, `usage.prompt_tokens`/`completion_tokens` (plus
`prompt_tokens_details.cached_tokens`) on Chat Completions, and
`usage.input_tokens`/`output_tokens` (plus
`input_tokens_details.cached_tokens`, including the
`response.completed` SSE event) on the Responses API. No token counts
are synthesized: a successful (2xx) request to a token-bearing endpoint
(`/api/chat`, `/api/generate`, `/api/embed`, the OpenAI-compatible
chat/completion/embedding/Responses paths) whose response carried no
counters — for example `/v1/*` streaming without a usage chunk —
increments `requests_without_usage` and contributes 0 tokens. (When
`total_tokens` accompanies exactly one explicit side, the other side is
derived; a total-only object is treated as missing.)
Metadata/management paths (`/api/tags`, `/api/show`, `/api/ps`,
`/api/embeddings`, …) and 3xx responses are never counted as "missing
usage". Streaming responses are sniffed line by line as they pass
through (bounded per-chunk work; no whole-body buffering), so latency is
affected only by that bounded scanning.

`range` scopes **only** the bucket series and `window_totals`;
`totals` and every dimension row are lifetime (`totals_scope`).

```json
{"source":"local","enabled":true,"version":2,"since":1756620000,
 "since_iso":"2025-08-31T06:00:00Z","started_at":1756620000,"uptime_s":3600,
 "saved_at":1756623600,"pending_flush":false,
 "persistence":"file","persistence_note":null,"last_flush_error":null,
 "range":"24h","granularity":"hour","totals_scope":"lifetime",
 "window":{"from":"2026-10-08T23:00:00Z","until":"2026-10-09T00:00:00Z"},
 "window_totals":{"requests":1,"input_tokens":2400,"output_tokens":300},
 "totals":{"requests":42,"ok":40,"errors":2,"requests_without_usage":3,
           "input_tokens":120000,"cached_input_tokens":0,"output_tokens":15000,
           "duration_ms":98765,"retries":1,"total_tokens":135000,"avg_ms":2351.5},
 "keys":[{"account":"alice","id":"3f2c9a0b1d4e5f6789abcdef01234567","suffix":"1234",
          "present":true,"requests":42,"input_tokens":120000,
          "output_tokens":15000}],
 "accounts":[{"account":"alice","requests":42}],
 "models":[{"model":"gpt-oss:120b","requests":42}],
 "endpoints":[{"endpoint":"api/chat","requests":42}],
 "by_status":{"200":40,"429":2},
 "buckets":[{"from":"2026-10-08T23:00:00Z","until":"2026-10-09T00:00:00Z",
             "partial":false,"requests":1,"input_tokens":2400}]}
```

Things to know:

- `present` distinguishes currently configured keys from historical rows
  kept after a keys-file change. Configured keys with no traffic yet
  appear with zero counters.
- The breakdown is **per key**: identity is a stable non-secret
  fingerprint (`id`) of the full key, so two keys under one `@account`
  are separable here even if their four-character display suffixes
  collide, which the upstream statistics endpoint cannot do.
- Counters are **persisted** by default at
  `$OLLAMUX_STATS_FILE`, else `$XDG_STATE_HOME/ollamux/stats.json`, else
  `~/.local/state/ollamux/stats.json` (a HOME-less environment falls back
  to `./ollamux-stats.json`). `--stats-file PATH` overrides the location;
  `--no-stats` (or `OLLAMUX_NO_STATS=1`) genuinely disables counting and
  persistence — `/_stats` reports `"enabled":false` with empty rows — and
  an explicit `--stats-file`/`--no-stats` wins over the environment
  variable. The file is written atomically every 5 s and after the
  shutdown drain; requests still in flight when the 5 s drain deadline
  expires are force-closed and their counters cannot be recorded. A
  corrupt, foreign, non-regular, newer-versioned or larger-than-16-MiB
  file (including a symlink) is left untouched (`persistence` reports
  `"blocked"`), so a mistyped `--stats-file` cannot silently destroy
  another file. A file is
  adopted only when it carries the `ollamux-stats` marker and the current
  schema version. Only recognized native/Chat/Responses usage shapes are
  read; valid JSON in an unknown wrapper is not scanned (avoiding false
  positives). Write failures are logged, surfaced as `last_flush_error`, and
  reported as `persistence:"error"` until a successful flush.
- Retention is fixed: 25 hourly and 31 daily buckets, pruned on load and
  on each recorded request, plus at most 512 historical key rows, 256
  accounts, 256 models and 128 endpoints (overflow folds into `<other>`).
  Currently configured keys are always retained even if they exceed the
  historical-key budget. Client-chosen paths cannot grow memory or the
  file without bound. There is no per-request log and no prompt/completion
  content.
- A second instance that tries to use the same stats file runs with
  `persistence:"blocked"` (advisory lock on unix; last-writer-wins without
  a lock elsewhere). Each flush also re-checks the target's marker/version
  and uses a unique temp file; cooperating instances are protected by the
  lock and the marker/version is rechecked before each rename.
- Local counters are additive to the upstream reporting above: they
  answer "which key and model served what, when", while `/_usage` answers
  "what does ollama.com think this account has consumed".

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
- `SIGINT` (ctrl-c) and `SIGTERM` (systemd/Docker stop) drain for up to
  5 s, then exit; signal again to force-quit. During the drain workers
  keep serving queued and in-flight requests until the deadline (an
  instance that stays idle for ~0.3 s exits early); anything still
  running at the deadline is force-closed.
- Request bodies are buffered up to 16 MiB (needed for replay across
  failover); larger bodies get `413`.
- Binds `127.0.0.1:11435` by default (`--addr` to change). The default port
  is *not* 11434 so it can run alongside a real local Ollama.

## Limitations (on purpose)

- Usage introspection against ollama.com is read-only reporting; there is
  no historical dashboard upstream — `/_usage` mirrors what ollama.com
  exposes per account (the only derived figure is the capacity-weighted
  pool `aggregate`, which ollama.com itself does not publish). Locally
  observed counters (requests, upstream-reported tokens, duration) live
  at `/_stats`; they are aggregated by key/account/model/endpoint and
  hour/day, with no per-request log or prompt content.
- No request rewriting: models must exist on ollama.com.
- A dead key stays dead until restart (`/_keys` shows why). Restarting is
  cheap: routing state is in-memory (only the local `/_stats` counters are
  persisted).
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
- SIGINT/SIGTERM shutdown is signal-mask based: the first signal starts
  the drain, the second force-quits (exit code 130).