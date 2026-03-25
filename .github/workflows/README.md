# dbt CI/CD — GitHub Actions Workflows

This project uses three GitHub Actions workflows to automate dbt validation,
deployment, and scheduling. Below is a complete explanation of the system.

---

## Architecture Overview

```
                      ┌─────────────────────────────────────┐
                      │           Developer                 │
                      └────────────┬────────────────────────┘
                                   │ opens PR
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  dbt_ci.yml  (Pull Request → master)                                     │
│                                                                          │
│  1. Write profiles.yml from secrets                                      │
│  2. dbt deps                                                             │
│  3. Download prod manifest.json  ◄── from CD artifact                   │
│  4. dbt compile          (syntax / ref() check)                          │
│  5. dbt run --select state:modified+ --defer --state ./prod-manifest     │
│  6. dbt test --select state:modified+ --defer --state ./prod-manifest    │
│  7. Drop PR schema (cleanup)                                             │
└──────────────────────────────────────────────────────────────────────────┘
                                   │ PR merged
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  dbt_cd.yml  (Push → master)                                             │
│                                                                          │
│  1. dbt source freshness                                                 │
│  2. dbt run  (full project, prod target)                                 │
│  3. dbt test --store-failures                                            │
│  4. dbt snapshot                                                         │
│  5. dbt docs generate                                                    │
│  6. Upload manifest.json as artifact  ──────────────────────────────►   │
│  7. Upload docs as artifact                                              │
└──────────────────────────────────────────────────────────────────────────┘

                    Every night at 03:00 UTC
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  dbt_scheduled.yml  (cron)                                               │
│                                                                          │
│  1. dbt source freshness                                                 │
│  2. dbt run  (incremental — processes only new rows)                     │
│  3. dbt test --store-failures                                            │
│  4. dbt snapshot                                                         │
│  5. Upload manifest.json as artifact  ──────────────────────────────►   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## One-Time Setup

### 1. Add GitHub Secrets

Go to: **Repository → Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Where to find it | Description |
|---|---|---|
| `DATABRICKS_HOST` | Workspace URL (no `https://`) | e.g. `dbc-aa8ddb8b-c135.cloud.databricks.com` |
| `DATABRICKS_HTTP_PATH` | SQL Warehouse → Connection Details | e.g. `/sql/1.0/warehouses/a84d06dd5092e3b1` |
| `DATABRICKS_TOKEN` | User Settings → Developer → Access Tokens | A PAT with permissions to write to `raw` database |

> **Security tip:** Create a dedicated Databricks service principal for CI/CD
> instead of using a personal token. Service principal tokens can be scoped to
> exactly the permissions needed and rotated without affecting your personal access.

### 2. Create GitHub Environments (recommended)

Go to: **Repository → Settings → Environments**

Create two environments:
- **`dev`** — no approval required (for CI runs)
- **`prod`** — add yourself as a "Required reviewer" (manual gate before every prod deploy)

This means a PR merge will pause and wait for your explicit approval before
running anything against production.

### 3. Enable GitHub Actions

Go to: **Repository → Settings → Actions → General** → Allow all actions.

---

## Slim CI — Detailed Explanation

The most important concept in this CI/CD setup is **Slim CI**.

### The problem with naive CI

If you run `dbt run && dbt test` on every PR, you rebuild the entire project
every time. For a project with 50+ models, that might mean:
- 10+ minutes of warehouse compute
- High cost per PR
- Slow feedback loop for developers

### The solution: `--state` and `--defer`

dbt compares the current PR's code against the last-deployed production state
(stored in `manifest.json`) to identify what actually changed.

```bash
# "Run only nodes whose code or config changed, plus everything downstream of them"
dbt run \
  --select state:modified+ \
  --defer \
  --state ./prod-manifest
```

**`state:modified+`** breaks down as:
- `state:modified` → nodes whose compiled SQL or config hash differs from manifest.json
- `+` (trailing) → all downstream dependents of those nodes

**`--defer`** means:
- For any node NOT in the selected set that is referenced as an upstream,
  dbt resolves its `ref()` to point at the **production** version of that table
  instead of expecting it to exist in the PR schema.

**`--state ./prod-manifest`** tells dbt where the reference manifest is.

### Example

Suppose you change only `fct_orders.sql`:

```
stg_jaffle_shop__orders  →  fct_orders (CHANGED)  →  dim_customers
stg_stripe__payments     ↗
```

With Slim CI:
- `fct_orders` is rebuilt (changed)
- `dim_customers` is rebuilt (downstream dependent, the `+`)
- `stg_jaffle_shop__orders` and `stg_stripe__payments` are **NOT rebuilt** —
  dbt reads them from their production tables via `--defer`

Without Slim CI, all 5 nodes would rebuild. Slim CI rebuilds 2.

---

## PR Isolation — Per-PR Schemas

Each CI run writes to a dedicated Databricks schema named `dbt_pr_<number>`:
- PR #42 → `raw.dbt_pr_42`
- PR #43 → `raw.dbt_pr_43`

This means multiple PRs can run CI simultaneously without interfering with each
other's tables. The `drop_schema` macro at the end of each CI run cleans up
the temporary schema automatically.

---

## manifest.json — The State File

Every `dbt run`, `dbt compile`, or `dbt docs generate` writes `target/manifest.json`.
It contains:
- Compiled SQL for every node
- A **content hash** of each node (used by `state:modified` to detect changes)
- Node dependencies (the DAG)
- Config metadata (materialization, tags, etc.)

The CD workflow uploads this file as a GitHub Actions artifact after every
successful production deploy. The CI workflow downloads it to compare against.

```
CD run → produces target/manifest.json → uploaded as "prod-manifest" artifact
CI run → downloads "prod-manifest" artifact → passes it to --state
```

On the very first run (no artifact exists yet), Slim CI gracefully falls back
to running the full project.

---

## `--full-refresh` Flag

Incremental models normally process only new rows. The `--full-refresh` flag
drops and recreates the table from scratch.

When to use it:
- When you add a new column to an incremental model
- When the incremental filter logic changes in a way that requires full reprocessing
- When a backfill is needed

How to trigger it manually:
1. Go to **Actions → dbt CD → Run workflow**
2. Set `full_refresh` to `true`
3. Click "Run workflow"

---

## Source Freshness

The `dbt source freshness` step checks that raw source tables have been updated
recently. Thresholds are configured in `_src_jaffle_shop.yml`:

```yaml
freshness:
  warn_after: {count: 24, period: hour}   # Yellow warning
  error_after: {count: 48, period: hour}  # Red error — blocks the deploy
```

If a source table hasn't been updated in 48 hours, the deploy fails. This
prevents you from building production models on stale data.

---

## `--store-failures`

When `dbt test --store-failures` runs in production, any failing test writes
the offending rows to a table in the `dbt_prod` schema:

```
raw.dbt_prod.not_null_fct_orders_order_id
raw.dbt_prod.accepted_values_stg_stripe__payments_...
```

You can then query these tables directly to investigate data quality issues
without having to re-run the test.

---

## Workflow Summary Table

| Workflow | Trigger | Target | Scope | Key flags |
|---|---|---|---|---|
| `dbt_ci.yml` | PR opened/updated | dev (PR schema) | Modified + downstream | `--defer --state` |
| `dbt_cd.yml` | Merge to master | prod | Full project | `--store-failures` |
| `dbt_scheduled.yml` | Nightly 03:00 UTC | prod | Full project | `--store-failures` |
