# Packaging notes

Everything needed to build/publish omlx packages lives here. The
release pipeline (`.github/workflows/release.yml`) runs all of this on
a `v*` tag push; this document explains the manual/one-time parts you
must configure once.

## Layout

| Path                        | What                                            |
| --------------------------- | ----------------------------------------------- |
| `omlx.1`                    | Man page (shared by AUR/RPM/deb)                |
| `omlx.service`              | Hardened systemd unit (shared)                  |
| `aur/omlx/`                 | AUR source package (builds from tag tarball)    |
| `aur/omlx-bin/`             | AUR prebuilt-binary package (GitHub release)    |
| `rpm/omlx.spec`             | Fedora/COPR spec (vendored offline build)       |
| `../debian/`                | Debian packaging (source deb, trixie rustc)     |
| `../Containerfile`          | Static musl → scratch container image           |
| `../scripts/vendor-source.sh` | Source tarball + vendored crates + checksums  |

## Release prerequisites (one-time, secrets — all optional)

**Every publishing channel auto-skips while its secret is missing**:
a release without any secrets still produces binaries, source/vendor
tarballs, the .deb, .rpm/.src.rpm, the GitHub release and the ghcr.io
container — the `aur`, `copr` and `cratesio` jobs print a notice and
succeed as no-ops. Add a secret later and re-run (see below).

1. **AUR push key** — create a dedicated key pair, register the public
   key on your aur.archlinux.org account, put the *private* key into the
   repo secret `AUR_SSH_KEY`:
   ```sh
   ssh-keygen -t ed25519 -C "omlx-aur-release" -f omlx-aur
   ssh -T git@aur.archlinux.org   # verify; also whitelists the host
   ```
   First push auto-creates the `omlx` / `omlx-bin` AUR packages. One key
   covers both packages (keys belong to the account, not the package).

2. **COPR** — create the project `j-stechmann/omlx` at
   copr.fedorainfracloud.org with chroots fedora-44, fedora-43,
   fedora-rawhide; export the API config (COPR → API → generate token)
   and save the whole file as repo secret `COPR_CONFIG` (it's written to
   `~/.config/copr` verbatim by CI).

3. **crates.io** — create a publish-scoped token, save as secret
   `CARGO_REGISTRY_TOKEN` (a `--dry-run` publish gates the real one).

4. **GHCR** — nothing to configure. CI pushes
   `ghcr.io/j-stechmann/omlx:<tag>` using `GITHUB_TOKEN` (job has
   `packages: write`). After the first release, flip package visibility
   to public in GitHub package settings if you want anonymous pulls.

## Publishing a deferred channel later

The workflow has `workflow_dispatch`, so once secrets are in place:

```sh
gh workflow run release.yml --ref v0.1.0
```

This re-runs the whole pipeline against the tag; build jobs redo
(cache-warmed), and the now-unlocked `aur`/`copr`/`cratesio` jobs
publish. Note: `workflow_dispatch` requires the trigger to exist in the
workflow file *at that ref* — if you re-ran a tag cut before this
feature existed, move the tag (`git tag -f`) or cut a `v0.1.1`.
You can also re-run just the failed/skipped jobs from the Actions UI
("Re-run failed jobs") after adding the secret, as long as the run is
recent.

## Releasing

```sh
git tag -a v0.1.0 -m "omlx 0.1.0"
git push origin v0.1.0
```

Everything else is automated; publishing channels run *if and only if*
their secret exists (see above). Watch the Actions run; the first
release works end-to-end even with zero secrets configured.

After the release, update the *repo copy* of both PKGBUILDs
(`pkgver`, checksums, regenerated `.SRCINFO`) so CI's freshness check
stays green — CI pushes the updated versions to the AUR, not to the
git repo. Loop closed by hand (or make it a small script later).

## Package contents (all formats)

- `/usr/bin/omlx`
- `/usr/share/man/man1/omlx.1`
- systemd unit `omlx.service` (`DynamicUser`, `LoadCredential` for
  `/etc/omlx/keys`; place root-0600 key lines there, then
  `systemctl enable --now omlx`)
- License: `GPL-2.0-or-later` (bundled CA roots:
  `CDLA-Permissive-2.0`; per-crate licenses ship in vendor archives)

## Distro-relevant facts an architecture reviewer will ask

- TLS: rustls + **bundled webpki-roots** (no system CA store needed;
  musl static binaries work anywhere, but corporate-MITM proxies can't
  be trusted via `/etc/ssl/certs` — feature `native-certs` of ureq
  would swap that, deliberately not enabled).
- ring compiles C: `gcc`/`clang` is a real BuildRequirement.
- `edition = "2024"`, `rust-version = 1.85`: Debian 13 trixie rustc
  1.85 builds it exactly; older stables would need a rustup bootstrap.