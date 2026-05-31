# Contributing to FlowWarden Stream Core

Thank you for your interest in contributing to FlowWarden Stream Core! Every contribution matters — whether it's a bug report, a feature suggestion, a documentation fix, or a code change.

Please note that this project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Ways to Contribute

> **Please open an issue before submitting a PR**, both for bug fixes and new features. This avoids wasted work on changes that may not align with the project direction, and keeps reviews focused.

- **Report bugs** — Open an issue with clear reproduction steps, expected vs. actual behavior, and your environment (Java version, Spring Boot version, MongoDB version). For non-trivial bugs, please provide a **runnable test case** (pushed to your fork) that isolates the issue. This dramatically speeds up triage and resolution.
- **Suggest features** — Open an issue describing the use case and why it would benefit the project.
- **Submit pull requests** — Code, tests, documentation improvements are all welcome.
- **Improve documentation** — Typos, unclear explanations, missing examples — every bit helps.

## Development Setup

### Prerequisites

- Java 17+ (tested on 17 and 21)
- Maven 3.8+
- Docker (required by Testcontainers)

### Clone & Build

```bash
git clone https://github.com/flowwarden-io/flowwarden-stream-core.git
cd flowwarden-stream-core
./mvnw clean verify
```

> **Note:** You do not need a local MongoDB installation. Testcontainers automatically provisions a MongoDB Replica Set during tests.

## Architecture Constraints

Before writing code, please be aware of the following architectural decisions:

- **Minimal dependencies (ARCH-010):** The core library depends only on Spring Data MongoDB and SLF4J. Any new dependency must be justified and approved via an issue before implementation.
- **Dual mode:** All code must work in both IMPERATIVE and REACTIVE modes.
- **Package structure:** All classes belong under `io.flowwarden.stream.*`.

## Coding Guidelines

- Code and comments in **English**.
- No Lombok in the core module (Lombok is allowed in samples only).
- Follow existing Spring conventions found in the project.
- Pure Java annotations — no XML configuration.
- Prefer simple, readable code over clever abstractions.
- All new `.java` files must include the Apache 2.0 license header (see `.license-header.txt`). Run `./mvnw license:format` to auto-add it.

## Testing

- **Unit tests:** JUnit 5 + AssertJ.
- **Integration tests:** Testcontainers with a MongoDB Replica Set.
- **Test profiles:** `test-mvc` (imperative) and `test-webflux` (reactive).
- **CI matrix:** Java 17 + Java 21 — both must pass.
- **Always** run `./mvnw clean verify` before submitting a PR.

## Commit & PR Guidelines

- Use [Conventional Commits](https://www.conventionalcommits.org/) format:
  `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`
- Open PRs against `main`.
- Describe the **why**, not just the what.
- Reference the related issue (`Closes #123`).
- One PR = one topic. Keep changes focused.
- **Update `CHANGELOG.md`**: any user-visible change (new feature, bug fix, breaking change, deprecation) must add an entry under the `[Unreleased]` section, in the appropriate category (`Added`, `Changed`, `Fixed`, `Deprecated`, `Removed`, `Security`). Internal-only changes (refactors, tests, CI) don't need a changelog entry.

### Squash strategy

- **Before opening the PR:** rebase on `main` and squash your work-in-progress commits into a small number of meaningful commits. The reviewer should see a clean history, not your local trial-and-error.
- **During code review:** do **not** squash. Each round of review feedback should land as its own commit (e.g., `review: rename variable X`). This lets reviewers verify only what changed instead of re-reading the whole PR. The final history is squashed automatically at merge time via "Squash and merge".

## Review Process

- Every PR is reviewed by at least one maintainer.
- CI must be green (Java 17 + Java 21) before merge.
- Changes to the public API require prior discussion via an issue.
- Be patient — we review as fast as we can, but this is a side project.
- Stale PRs and issues with no activity for an extended period may be flagged and eventually closed. Reopening is always welcome with fresh context.

## License

All contributions are submitted under the [Apache License 2.0](LICENSE). By opening a pull request, you agree that your contribution will be licensed under this same license.

The CI build runs `./mvnw license:check` to ensure every Java source file carries the Apache 2.0 header. If you add a new file, run `./mvnw license:format` before submitting your PR.

## Developer Certificate of Origin (DCO)

We use the [Developer's Certificate of Origin 1.1 (DCO)](https://developercertificate.org/) — the same lightweight mechanism used by the Linux kernel and many other open source projects — instead of a formal CLA.

Each commit must include a `Signed-off-by` line in its message:

```text
Signed-off-by: Jane Doe <jane.doe@example.com>
```

Add it automatically by committing with the `-s` flag:

```bash
git commit -s -m "feat: add new feature"
```

Or configure git once to sign every commit:

```bash
git config --global format.signOff true
```

A DCO bot enforces this on every pull request. Commits without the sign-off will block the merge.
