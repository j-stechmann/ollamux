# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-09-09

### Changed

- Breaking: the `/_usage` `aggregate` is now the **capacity-weighted
  mean** of the reporting keys' usage fractions (each fraction weighted
  by its tier's cap — free ×1, pro ×50, max ×250 — summed and divided by
  the summed weights of exactly the keys that reported the window), so
  it stays in 0.0–1.0 like the per-key rows: a single-key pool
  reproduces that key's own fraction and consumers of the per-key range
  need no adjustment. Previously the aggregate was the weighted *sum* in
  free-plan-cap equivalents (e.g. 202.537 for a free key at 3.7% plus a
  max key at 81%; the same pool now reports 0.807, "pool capacity
  fraction"). Error rows still contribute nothing and their weight is
  excluded from the denominator; all other semantics (cooldown keys
  included, unreported windows `null`, 3-decimal rounding) are
  unchanged. The `unit` field changed from `free-plan cap equivalents`
  to `pool capacity fraction`.

## [0.6.0] - 2026-09-06

### Added

- `/_usage` now reports a pool-wide aggregate alongside the per-key rows:
  each key's usage fraction (a fraction of *its own* plan cap) is weighted
  by the plan tier inferred from its per-key concurrency (`KEY:N` — free=1
  → ×1, pro=3 → ×50, max=10 → ×250; more concurrency than normal counts as
  the next tier up) and summed, so the total reads in free-plan-cap
  equivalents. All keys contribute, including ones on cooldown (usage
  fetching never consults key health); dead keys are expected to fail
  their own usage fetch the same way and contribute nothing. Rounded to 3
  decimals; windows nobody reported are `null`, never 0.
- `/_usage` rows and `/_keys` rows now carry the key's inferred plan
  `tier` (`free`/`pro`/`max`).

## [0.5.0] - 2026-09-01

### Added

- Prompt-cache affinity, on by default: conversation identity (model +
  leading system messages + first non-system message for chat; model +
  first 8 KiB of the prompt for generate/completions) is pinned to the
  API key that warmed ollama.com's server-side prompt cache — same-
  conversation requests reuse one key instead of round-robining, for
  faster first tokens. Best-effort: a busy/dead/cooling/over-quota
  pinned key never blocks; the request routes normally and the next
  success re-pins. New `X-Ollamux-Affinity: hit|miss|off` header on
  key-served responses; `aff=` field in verbose logs. Disable via
  `--no-affinity` or `OLLAMUX_NO_AFFINITY=1`.

### Fixed

- `OLLAMUX_NO_AFFINITY=1` is now honored on its own (the env check was
  nested under the `--no-affinity` branch, so the env var alone did
  nothing).
- `X-Ollamux-Affinity` now reports `miss` (not `hit`) when a pin existed
  but its key could not serve (dead, cooling, over-quota, or full) and
  the request fell through — matching the documented semantics.

## [0.4.0] - 2026-09-01

### Added

- `/_usage` endpoint: per-key Ollama Cloud usage (session/weekly
  fractions of the plan cap, percents, top models, 4-week rolling cost)
  via the undocumented `GET /api/usage` upstream endpoint — parallel
  fan-out across all keys, 60 s cache, `?refresh=1` to force a refresh
  (rate-limited to one fetch *attempt* per 5 s, so failed rounds back
  off too), suffixes only (no secrets). Payload drift on the
  undocumented endpoint degrades to a per-key error string, never a
  crash; usage checks never touch key health.
- `/_keys` now embeds the latest known usage per key (`usage` field)
  from the shared snapshot — still a pure in-memory read that never
  triggers upstream calls. Windows absent from the snapshot are omitted
  rather than substituted from the other window.
- Quota-aware key selection behind `--usage-aware[=PCT]` (default 80;
  also `OLLAMUX_USAGE_AWARE`, flag wins): keys at/over the threshold
  session usage are demoted in candidate selection — served last, never
  excluded — with a 60 s background poller; off by default and a no-op
  without usage data.

### Removed

- Obsolete `Ideas.md` document.

## [0.3.0] - 2026-08-30

### Added

- `X-Ollamux` identity header on proxied responses (unified across
  streaming and non-streaming), so clients can confirm when a response
  came through ollamux.
- Agent-friendly error messages: upstream errors are classified into
  actionable text (bad key, rate-limited, upstream unavailable, model
  not found) instead of opaque bodies.

### Changed

- Logging is silent by default; `-v` enables the startup banner, the
  request log and health/state notices.

## [0.2.0] - 2026-08-30

### Changed

- Rebrand: `omlx` is now `ollamux` (Ollama multiplexer).
- Renamed environment variables, HTTP headers, error codes and config
  paths accordingly (`OLLAMUX_KEYS`, `X-Ollamux-*`,
  `~/.config/ollamux`).

## [0.1.0] - 2026-08-30

### Added

- First packaged release.
- Distro packaging: AUR (`ollamux`, `ollamux-bin`), COPR/RPM with
  systemd unit and man page, deb, and container image.
- CI release workflow with secret-gated publishing: AUR, COPR,
  crates.io and GitHub release assets (container + RPM) are skipped
  cleanly when secrets are absent; re-runnable via
  `workflow_dispatch`.

## [0.0.1] - 2026-08-29

### Added

- Initial `omlx`: key-rotating reverse proxy for the Ollama Cloud API —
  round-robin key pool with health tracking, no-auth fast path,
  streaming passthrough.

### Fixed

- Audit findings: SIGINT masking during shutdown, no-auth request
  paths, request body truncation, and health reset semantics.

[Unreleased]: https://github.com/j-stechmann/ollamux/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/j-stechmann/ollamux/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/j-stechmann/ollamux/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/j-stechmann/ollamux/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/j-stechmann/ollamux/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/j-stechmann/ollamux/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/j-stechmann/ollamux/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/j-stechmann/ollamux/compare/0.0.1...v0.1.0
[0.0.1]: https://github.com/j-stechmann/ollamux/releases/tag/0.0.1