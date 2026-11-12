# CLAUDE.md

Guidance for Claude Code and other agents working in `lyric-ecosystem`.

## What this repository is

Out-of-tree Lyric libraries: backends and integrations that bind to an external SDK, service or
runtime (Quartz, RabbitMQ, Kafka, S3, Azure Blob, Hangfire, Elasticsearch drivers, and similar).
The language, compiler, standard library and the target-neutral library interfaces live in
`nichobbs/lyric-lang`. This repo is the "extraction" phase of `lyric-lang/docs/71-ecosystem-remediation-plan.md`
(D552, monorepo decision D561).

Rule of thumb: if it needs a third-party SDK or a native dependency to be real, it belongs here.
If it is pure Lyric, or it defines the interface and conformance suite, it belongs in `lyric-lang`.

Read before starting: `lyric-lang/docs/71-ecosystem-remediation-plan.md`, `docs/39-package-registry.md`,
`docs/38-workspace.md`, `docs/21-nuget-linking.md`, `docs/31-maven-linking.md`.

## Layout

```
lyric.toml                  # [workspace] root, members auto-discovered
<interface>-<backend>/      # one package per backend, e.g. jobs-quartz/, mq-rabbitmq/, storage-s3/
  lyric.toml
  README.md                 # supported targets, stability, SDK versions, known gaps
  src/                      # public API (thin) + implementation
  src/_kernel_<target>/     # externs only, per-target kernels (see "Kernels")
  tests/                    # conformance run + backend-specific tests
  tests/integration/        # needs a real service (container); separate CI job
```

One package per backend. Never one package that bundles several backends, and never a package
whose public API exposes a backend that is not implemented.

## Backend-specific libraries (the key rule)

Many backends exist on one target only (Quartz is JVM-only, Hangfire is .NET-only, grpc-java, Npgsql...).
That is expected and legitimate. Handle it as follows.

1. **The target-neutral interface stays in `lyric-lang`.** `JobScheduler`, `MessageQueue`,
   `StorageBucket`, `CacheStore` and friends are defined there with a conformance suite in
   `lyric-testing`. A backend here implements the interface; it does not redefine it.
2. **Name by interface and backend**: `lyric-jobs-quartz`, `lyric-mq-rabbitmq`, `lyric-storage-s3`.
3. **Declare supported targets in the manifest.** Each backend's `lyric.toml` carries a `[package]
   targets = ["jvm"]` field (subset of `dotnet`, `jvm`, `native`), mirrored in the README. A JVM-only
   package is a first-class citizen, not a gap. The field does not exist in `Lyric.Manifest` yet: it is
   a `lyric-lang` feature to add first (manifest parser, `lyric build`/`lyric test`/`lyric publish`
   enforcement, registry metadata for `lyric search`), tracked as an issue there. Until it lands, gate
   the whole package behind a feature (as `lyric-lambda` does with `feature = "jvm"`) and record the
   intended `targets` value in the README.
4. **Fail loudly on an unsupported target.** Building a package with a `--target` outside its `targets`
   must produce a clear build-time diagnostic (not a runtime failure), and a dependent application
   resolving it for the wrong target must fail at restore/build. Never ship a stub that returns
   `NOT_IMPLEMENTED`/`NotImplemented` at runtime (D552 removed these from public APIs; do not
   reintroduce them).
5. **Same capability, two SDKs = two packages, one interface.** If a capability has a good backend on
   each target (for example Quartz on JVM, Hangfire or Quartz.NET on .NET), publish
   `lyric-jobs-quartz` and `lyric-jobs-hangfire` separately. Applications depend on the interface and
   select the backend in their own wiring (`wire`/`config`); do not hide the choice behind
   target-conditional imports inside a single package.
6. **Parity is per interface, not per backend.** The requirement is that every backend passes the
   conformance suite on every target it claims. It is not required that every backend exist on every target.
7. **No lowest-common-denominator API.** Backend-specific features (Quartz cron triggers, S3 presigned
   multipart, Kafka partitions) go in a backend-specific module of that package, clearly separate from
   the interface implementation.

## Kernels and FFI

- Use `extern type` plus auto-FFI (`import extern`, `.new()`; docs/42, 47, 48). Do **not** use
  `extern package` (it never resolves to a real binding; see the lyric-feature-flags history, #5324).
- Extern declarations live only in kernel files (`_kernel_*`), with the public Lyric API on top.
  No hand-routed BCL/JDK calls from public modules.
- Native SDK dependencies are declared in `[nuget]` / `[maven]` / `[native]` of the package manifest,
  pinned to exact versions. Record the pinned SDK version in the README.
- **No F# code, no C# or Java shims.** Everything is Lyric. If a needed capability cannot be
  reached through auto-FFI, fix the compiler in `lyric-lang` first (open an issue and link it);
  do not add a host shim here.

## Production-readiness standard

Same bar as `lyric-lang`: nothing here is bootstrap-grade.

- A backend is only published when it passes the full conformance suite for each claimed target, in CI,
  against a real service (container) for integration tests plus a fake/in-process variant where possible.
- A previous attempt that never ran application code is the cautionary tale (the old Quartz kernel
  never invoked application handlers and CI never selected it). Tests must run real handlers end to end.
- No skipped, ignored or disabled tests to get green. No placeholder diagnostics. No `TODO` landings.
- If the proper version is larger than the task, ship a smaller finished slice and file a tracked issue.
- Errors map to the interface's `Result[T, E]` error type; SDK exceptions never escape the public API.
- Secrets and connection strings come from `config { }`, never from source or test fixtures.

## Stability and versioning

- Each package is independently versioned (SemVer). Start at `0.x` with `stability = "experimental"`
  (once `[package] stability` lands, Phase 2.1 of docs/71); promote only when the conformance suite
  and integration tests are green on every claimed target.
- Packages version and release independently of each other and of the compiler.
- A backend declares compatibility against the **interface package version** it implements (a SemVer
  range on the interface dependency, e.g. `lyric-jobs = ">=1.2, <2"`), not against a specific `lyric`
  compiler version. The conformance suite version is pinned in the backend's CI to the lower bound of
  that range and also run against the newest release in it (Q-ECO-002). Bump deliberately.
- Interface packages must follow SemVer strictly: any change that breaks an implementor (new required
  method, changed signature, tightened conformance behaviour) is a major bump.
- The compiler is a build input, not a compatibility axis: CI uses a pinned `lyric` release recorded in
  one place (`.lyric-version` at the repo root), bumped in its own PR. Do not encode compiler versions
  in package manifests unless a package needs a specific language feature, in which case state the
  minimum and why in the README.
- Publishing flows through `lyric publish` to the registry (docs/39). Do not hand-roll release scripts.

## Working conventions

- Toolchain: use the installed release `lyric` (`scripts/install.sh` in `lyric-lang`). You only need
  `make lyric` from `lyric-lang` when you are changing the compiler itself, which is a separate PR in
  that repo.
- Format every changed `.l` file with `lyric fmt --write` before committing. A refusal means a
  formatter/parser bug: fix it in `lyric-lang` or leave the file unstaged. Never hand-format around it.
- Style: no emojis, no new markdown documents unless asked, prefer editing existing files, comments only
  for non-obvious why.
- Test per package: `lyric test --manifest <package>/lyric.toml --target <dotnet|jvm>`. CI runs each
  package on exactly the targets it claims. Integration tests run in a separate job with containers.
- Compiler bugs found here: reproduce minimally, open an issue in `nichobbs/lyric-lang`, link it in the
  package README under "Known gaps", and do not work around it in library code.

## Docs to update with every change

1. The package README (targets, stability, SDK versions, known gaps).
2. The ecosystem matrix (book chapter 29 in `lyric-lang`) via a PR there if status changed.
3. A progress note in `lyric-lang/docs/progress/` for shipped milestones. The decision log is
   append-only, one file per entry; this repo does not carry its own.

## Branches and PRs

- Develop on the branch assigned by the session prompt; never push elsewhere without permission.
- Before opening a PR: `git fetch origin main && git rebase origin/main`, resolve conflicts, push
  (`--force-with-lease` after a rebase).
- Open PRs ready for review, not draft. After opening, subscribe to PR activity and fix CI and review
  findings until green.
- One backend per PR where practical. Cross-repo changes (interface, conformance suite, compiler) land
  in `lyric-lang` first and are linked from the backend PR.
