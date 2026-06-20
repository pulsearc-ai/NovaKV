# Contributing to NovaKV

Thanks for your interest in improving NovaKV! This document covers how to
build, test, and submit changes.

## Getting started

NovaKV is a standard Cargo workspace. You need a recent stable Rust
toolchain (install via [rustup](https://rustup.rs)).

```sh
# Build everything
cargo build --workspace

# Run the test suite
cargo test --workspace --all-features
```

The default build pulls in no third-party crates; the optional `snappy`
feature adds the pure-Rust [`snap`](https://crates.io/crates/snap) crate.
Please keep the default build dependency-light.

## Before you open a pull request

CI runs the following checks — please run them locally first:

```sh
# Formatting (must be clean)
cargo fmt --all -- --check

# Lints (warnings are treated as errors)
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Tests
cargo test --workspace --all-features

# Docs build cleanly
cargo doc --workspace --all-features --no-deps
```

A quick benchmark harness lives under `core/benches`:

```sh
cargo bench -p novakv --bench db_latency
```

## Guidelines

- **Match the surrounding style.** The codebase favors small, focused
  modules and thorough `//!`/`///` docs — please document new public
  items.
- **Keep changes scoped.** Small, reviewable PRs land faster. If you're
  planning a large change, open an issue to discuss it first.
- **Add tests** for new behavior and bug fixes.
- **Preserve the on-disk format.** NovaKV is compatible with LevelDB's
  format; changes that affect it need a strong rationale.

## Licensing of contributions

NovaKV is distributed under `(MIT OR Apache-2.0) AND BSD-3-Clause` (see
[README](README.md#license)). By submitting a contribution, you agree
that it is licensed under the same terms.

Please sign off your commits to certify the
[Developer Certificate of Origin](https://developercertificate.org/):

```sh
git commit -s -m "your message"
```

## Reporting bugs and requesting features

Use the GitHub issue tracker. For **security vulnerabilities**, do not
open a public issue — see [SECURITY.md](SECURITY.md).
