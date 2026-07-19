# Delivery Reliability Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a deterministic seven-day Weather/Traffic run-level delivery reliability report from normalized read-only evidence.

**Architecture:** A dependency-free common package validates one evidence row per `domain × scheduled_run_id`, classifies Bronze/transform/Gold outcomes, and aggregates supply, delivery, SLA, completeness, freshness, and recovery metrics. A CLI reads a normalized JSON evidence bundle and emits JSON, CSV, or Markdown without writing to Airflow, R2, Trino, or warehouse tables.

**Tech Stack:** Python 3, dataclasses, argparse, json, csv, pytest

## Global Constraints

- Base branch is `origin/dev`; work only on `feat/431-delivery-reliability-pilot`.
- Do not change collection, manifest, transform, Gold model, schedule, or production state.
- Preserve `SUCCESS + is_publishable` as the Bronze publication gate.
- Preserve unavailable evidence as `NOT_AVAILABLE`; never coerce it to zero.
- Use UTC timestamps internally and deterministic ordering in every output.

---

### Task 1: Evidence contract and classification

**Files:**
- Create: `common/delivery_reliability/__init__.py`
- Create: `common/delivery_reliability/contract.py`
- Test: `common/tests/test_delivery_reliability_contract.py`

**Interfaces:**
- Consumes: normalized dictionaries from a pilot evidence JSON document.
- Produces: `DeliveryEvidence.from_mapping`, `DeliveryState`, and validated UTC timestamps.

- [ ] Write tests for valid delivered, zero-row, partial, source failure, contract failure, missing evidence, and duplicate grain.
- [ ] Run the tests and confirm they fail because the package does not exist.
- [ ] Implement the smallest immutable evidence contract and classifier.
- [ ] Run the tests and confirm they pass.

### Task 2: Seven-day aggregate metrics

**Files:**
- Create: `common/delivery_reliability/aggregate.py`
- Test: `common/tests/test_delivery_reliability_aggregate.py`

**Interfaces:**
- Consumes: validated `DeliveryEvidence` rows.
- Produces: `build_pilot_report(rows)` with rates, latency distribution inputs, state counts, and MTTR recovery events.

- [ ] Write tests for supply rate, delivery rate, SLA rate, completeness, freshness, and failure-to-next-delivery MTTR.
- [ ] Run the tests and confirm the aggregation API is missing.
- [ ] Implement deterministic aggregation with `None` for unavailable metrics.
- [ ] Run the tests and confirm they pass.

### Task 3: Verifiable pilot outputs

**Files:**
- Create: `common/delivery_reliability/render.py`
- Create: `scripts/delivery_reliability_pilot.py`
- Test: `common/tests/test_delivery_reliability_render.py`

**Interfaces:**
- Consumes: a JSON object containing `runs` plus an output format.
- Produces: stable JSON, CSV, or Markdown containing run IDs, timestamps, final row counts, states, summary rates, and MTTR.

- [ ] Write tests for stable JSON, CSV, and Markdown plus invalid duplicate input.
- [ ] Run the tests and confirm rendering/CLI behavior is missing.
- [ ] Implement renderers and a read-only CLI.
- [ ] Run all new and existing targeted tests.

### Task 4: Verification and handoff

**Files:**
- Modify only the files introduced by Tasks 1–3 if verification requires corrections.

**Interfaces:**
- Consumes: complete branch diff.
- Produces: Gate B evidence and a remote feature branch for user validation.

- [ ] Run changed-path compileall and targeted pytest.
- [ ] Run `git diff --check` and a diff-based secret scan.
- [ ] Review the diff for read-only behavior and domain boundaries.
- [ ] Report Gate B before commit and push.
