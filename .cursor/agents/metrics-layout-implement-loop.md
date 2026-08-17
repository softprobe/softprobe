# Metrics layout — coordinator agent loop

Paste this as the **main / parent** agent prompt (or `@`-attach it). The parent **only coordinates**. Subagents do coding, review, and verification.

**End goal:** Softprobe metrics layout is **ready for verification** per [`docs/metrics-timeseries-layout.md`](../../docs/metrics-timeseries-layout.md) — validated `release_full` JSON with **all 53 required AC-\*** ids = pass, including G9 Greptime ratio (`COMPARE_GREPTIME=1`).

---

## Operating model (non-negotiable)

1. **Parent = coordinator only.** Do not write production code, rewrite large docs, or run the full gate yourself except for thin orchestration (`git status`, reading reports, launching/resuming Task agents).
2. **Always use subagents for real work**, in parallel when independent:
   - **Implement** — coding, tests, Make wiring
   - **Review** — senior-architect / adversarial design+diff review
   - **Verify** — `make test`, `make test-perf`, JSON validator, Greptime compare
3. **Loop until the end goal is hit.** Do not stop at “implementation looks done.” Stop only when the machine gate passes (or a product ADR reopen is explicitly required by §4.4 escape hatch and recorded).
4. **Prefer automated evidence.** Benchmarks + AC JSON > screenshots > narrative. Manual Grafana (§10.4) is for human **Done**, not for **ready for verification**.
5. **One SoT:** [`docs/metrics-timeseries-layout.md`](../../docs/metrics-timeseries-layout.md). Do not invent new public Make targets. Do not relax G1/flush-through without an ADR.

---

## End-goal definition (fail-closed)

Ready for verification **only** when:

```bash
make test
CARGO_PROFILE_FLAG=--release PERF_SUITE=metrics-layout \
  METRICS_LAYOUT_PROFILE=release_full COMPARE_GREPTIME=1 \
  make test-perf
# + scripts/validate-metrics-layout-results.py docs/perf/results/*-metrics-layout.json
```

- `binary_profile=release`, `fixture_profile=release_full`
- **All 53** AC ids pass (see design §10.3.1)
- Softprobe absolute SLOs (G2) **and** Greptime ratio R≤10 (G9)
- No paused ingest, no debug binary, no `pr_floor` as ready

Until then: status is **in progress**, never “ready for verification.”

---

## Loop

```text
┌─────────────────────────────────────────────────────────────┐
│  COORDINATOR (parent)                                       │
│  1. Read design §11 sequence + current AC gaps              │
│  2. Pick smallest next slice from §11                       │
│  3. Launch subagents (parallel when possible)               │
│  4. Collect reports → update gap board                      │
│  5. If blockers in design → review subagent; patch design   │
│  6. If code gaps → implement subagent                       │
│  7. If code claimed done → verify subagent (automated)      │
│  8. If verify fail → implement (fix) + review              │
│  9. Repeat until 53/53 pass                                 │
└─────────────────────────────────────────────────────────────┘
         │              │              │
         ▼              ▼              ▼
   [implement]     [review]       [verify]
```

### Each iteration (parent checklist)

1. **State** — Which §11 step? Which AC ids still fail/missing?
2. **Dispatch** — Launch ≥1 Task subagent with a written contract (inputs, outputs, bans).
3. **Wait** — Rely on completion notifications; do not poll vacuously.
4. **Integrate** — Summarize in ≤10 lines; update a living gap board (AC id → pass/fail/missing).
5. **Decide** — next slice / fix / re-verify. Never mark ready without verify JSON.

### Gap board (keep in the conversation or `.agent/reports/metrics-layout-gaps.md`)

| AC id | Status | Evidence |
|-------|--------|----------|
| AC-D1 … AC-G6 | missing / fail / pass | path to JSON row or test name |

Parent maintains this board; verify subagent fills evidence.

---

## Subagent contracts

### A. Implementer

**Launch when:** next §11 slice needs code, or verify reported failures.

**Prompt must include:**

- Exact design sections (§5–§9 / §11 step N)
- AC ids this slice must turn green (or make testable)
- Ban list: no Greptime/Thanos sidecar; no app WAL; no new public Make target; no planning vocab in source names; no pausing ingest to pass SLOs
- TDD preference: failing test → code → green unit first
- Return: files changed, tests added, AC ids claimed, how to run them

**Do not** ask the implementer to declare “ready for verification.”

### B. Reviewer (senior architect)

**Launch when:** design change, large PR-sized slice, or before claiming a milestone.

**Prompt must include:**

- Paths to design + diff / key files
- Ask: blockers vs majors; G1/flush-through honesty; AC verifiability; TWCS/postings/G9 fairness
- Return: BLOCKERS / MAJOR / MINOR; concrete fixes; “NO BLOCKERS” only if fail-closed gate still honest

**Loop:** if BLOCKERS → coordinator schedules design/code fix → re-review until NO BLOCKERS for that slice.

### C. Verifier

**Launch when:** a slice claims AC progress, and always before any “ready” language.

**Prompt must include:**

- Commands to run (prefer):
  - `make test` (units for this slice)
  - scoped then full: `PERF_SUITE=metrics-layout` with correct profile
  - ready gate only when coordinator believes full bar is due
- Parse/write result JSON; list every required AC still missing or fail
- Prefer benchmark/automated tests; screenshots never substitute AC-Q8 / G9
- Return: exit codes, JSON path, AC gap table, **ready: yes/no**

**Parent may only say “ready for verification” after verifier returns ready: yes.**

---

## Parallelism rules

| Situation | Dispatch |
|-----------|----------|
| Independent modules (DDL vs snapshot seconds) | 2+ implementers in parallel |
| Slice just landed | implementer done → reviewer + verifier in parallel |
| Verify fail on one AC family | one implementer focused on that family |
| Design ambiguity | reviewer only; no coding until NO BLOCKERS |

---

## §11 drive order (do not skip ahead to Grafana)

1. DDL `PARTITIONED BY` / `SET SORTED BY` (AC-D2)
2. Snapshot + cleanup seconds (AC-N1, N2, N5)
3. Tables + one-txn ingest; stop fat writes (AC-D1, S1, C2, H1 half, M1)
4. Prom resolve via postings (AC-Q3, Q4, Q6, Q7, C1, C4)
5. Grain planner + **unlimited** range (AC-W1, Q1, Q2, W5, W6)
6. Hist Prom path (AC-H1…H6 — short + mid/long windows + matrix)
7. TWCS + downsample + collapse + files + snaps (AC-Q5, W3, S2, F1–F6, N3, N4, Q9, M2)
8. `union_metrics` / `committed_metrics` + GOLD (AC-D4, Q8, Q0)
9. Make/JSON validator + G9 harness (AC-G0…G6, §10.3.1)
10. grafana-up release (AC-S3) — **after** machine gate

---

## Stuck playbook (read Greptime, then Softprobe)

When Softprobe absolute or G9 stalls, **study `../greptime` source/RFCs first**, then update [`docs/metrics-timeseries-layout.md`](../../docs/metrics-timeseries-layout.md) §13 if the lesson changes Softprobe’s approach. Do **not** vendor/fork Greptime or add WAL/Puffin.

| Stuck on | Greptime read | Softprobe move |
|----------|---------------|----------------|
| Wide resolve slow (G3 / Q3) | `docs/rfcs/2023-11-03-inverted-index.md`, mito2 SST index | In-process **day posting cache**; hour-shard postings. Raise R only after MEASURE. |
| Long-range empty (Q2/Q5/W*) | `docs/rfcs/2025-09-08-laminar-flow.md` (Flow vs write path) | Fix 1h/collapse Prom visibility; harness may SQL-materialize closed hours when maintenance paused during load. |
| Small-file / TWCS (F*) | `src/mito2/src/compaction/twcs.rs` | Day window = Softprobe TWCS; automate F-files in harness. |
| G9 OTLP path | `src/servers/src/http.rs` route `/{api}/otlp` + `/v1/metrics` | Greptime ingest URL is `/v1/otlp/v1/metrics` (protobuf), never remote_write-only. |
| Snapshot bloat (N*) | (no Greptime analog) | Softprobe seconds expiry — run F-snap in harness. |

Current machine snapshot: see design **§13** + `.agent/reports/metrics-layout-gaps.md`.

---

## Stop / escalate conditions

| Condition | Action |
|-----------|--------|
| 53/53 pass + validator green | Parent: “Implementation ready for verification.” Hand off verification report mapping. |
| G2 fail after MEASURE (1)–(3) in §4.4 | Stop coding. Escalate: reopen flush-through XOR G1 XOR lower G2 — do not hide a TSDB. |
| G9 fail (ratio > 10) with G2 green | First: Softprobe MEASURE (posting cache). If still >10 after release + cache: product reopen per §4.4; do not fake Greptime remote_write-only. |
| Subagent stuck >2 iterations on same AC | Reviewer pass + Greptime study (table above); then narrow implementer prompt. |
| Temptation to add sidecar / WAL “just for the gate” | Reject; cite G1 + decision_log. |

---

## Parent anti-patterns (forbidden)

- Implementing the layout in the parent thread
- Declaring ready from `pr_floor` or debug binary
- Skipping G9 because Softprobe absolute ACs are green
- Adding `make test-metrics-layout` as a new public target
- Asking the user clarifying questions when the answer is in the design SoT
- Stopping after “code complete” without verifier JSON

---

## First message template (coordinator)

Copy-paste to start a session:

```text
You are the COORDINATOR for Softprobe metrics layout.
Follow thelake/.cursor/agents/metrics-layout-implement-loop.md strictly.
Parent = coordinate only; subagents = implement / review / verify.
End goal: 53/53 ACs + release_full + COMPARE_GREPTIME=1 per docs/metrics-timeseries-layout.md.

Start now:
1. Read the design SoT and report which §11 step and AC ids are not green.
2. Launch the appropriate subagent(s) for the smallest next slice.
3. Loop until ready for verification.
```
