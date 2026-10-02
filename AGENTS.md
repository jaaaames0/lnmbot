# Agent guidance for lnmbot

This file applies to the entire repository. It supplements
`/home/james/AGENTS.md`, which covers the production host and still applies to
infrastructure, deployment, credentials, and service work. User instructions
for the current task take precedence over this file.

## Repository scope

- This checkout is editable source, not the running release. The live trader
  and dashboard use separate immutable releases, protected env files, and a
  shared SQLite state directory. Do not infer their current version or state
  from the checkout alone.
- Start with `README.md` for the product model, `DEPLOYMENT.md` for operator
  procedures, `CHANGELOG.md` for dated release history, and `pyproject.toml`
  for tests and tooling. Treat historical release facts as dated evidence.
- `docs/`, `scripts/research/`, and `tests/research/` are local, Git-ignored
  research or operations archives. Preserve them when cleaning the tree. The
  tracked `config/seeds/` files are runtime inputs, not disposable research.

## Trading behavior to protect

- The live runner always manages independent `1d` and `4h` MA positions. The
  optional funded close-range strategy shares account and risk controls but
  has its own campaign and unit ownership. Historical seeds and the separate
  shadow book must stay out of funded positions and P&L.
- Keep entry admission, exposure-reducing exits, and whole-run halting distinct.
  A rejected or ambiguous entry must not create duplicate exposure; an entry
  block must not silently suppress an owned exit. Reconcile venue inventory,
  durable commands, funding, and accounting across restart paths.
- Respect the import boundaries in `pyproject.toml`: strategies do not import
  the API, live data/engine, or persistence; engines do not import
  `lnmarkets_bot.api.trades` directly.
- Changes to sizing, risk, order submission, reconciliation, funding, or
  strategy state need focused tests with synthetic data, fake venue responses,
  and temporary databases. Never use live credentials or submit a real order
  as part of an ordinary test run.

## Working and validation

- Check `git status --short` before edits and preserve unrelated work. Prefer
  narrow changes and inspect callers and consumers before moving or removing
  files.
- Run focused tests while iterating. For production Python changes, finish
  with the default `uv run pytest -q` suite and scoped Ruff checks for changed
  Python files. The default suite excludes `tests/research/`; archived research
  checks can require local market data and are run only when relevant.
- Let test commands finish and report the final result. If a run stalls,
  identify the process and cause before stopping it. Documentation-only edits
  need link and diff checks, not a full test run.
- Keep `README.md` current for the operating model, `DEPLOYMENT.md` for actual
  procedures and configuration, and `CHANGELOG.md` for changes and releases.
  Keep new research evidence in the ignored local archive unless explicitly
  asked to publish it.

## Strategy reviews

- For a quarterly strategy audit, start with the local
  [review protocol](docs/research/2026-10-01-strategy-review-protocol/README.md)
  and follow its workflow, templates and evidence/action registers. If the
  ignored archive is absent, report the missing protocol rather than inventing
  its thresholds.
- Carry prior adverse evidence, unresolved actions and frozen baselines into
  each review. Distinguish insufficient evidence from meeting expectations.
  A losing quarter alone does not justify retuning. Audit requests authorize
  local research and read-only inspection; funded changes follow their own
  operator authorization and deployment procedure.

## Live host boundary

A source-editing task does not itself call for a live service or database
change. For a deployment or host-operation task, read `/home/james/AGENTS.md`
and its referenced system topology and operator handbook, verify the actual
release and service state, and follow the applicable change procedure. Do not
print env-file contents or credentials. Treat service restarts, funded orders,
ledger repairs, and production database writes as live operations rather than
local validation.
