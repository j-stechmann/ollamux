# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/j-stechmann/ollamux/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/j-stechmann/ollamux/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/j-stechmann/ollamux/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/j-stechmann/ollamux/compare/0.0.1...v0.1.0
[0.0.1]: https://github.com/j-stechmann/ollamux/releases/tag/0.0.1