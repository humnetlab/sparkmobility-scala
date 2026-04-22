# Plan: sparkmobility-scala perf cleanup, dead-code removal, README refresh

## Context

The sparkmobility-scala repo (Scala library backing the Python `sparkmobility` package) was just audited in this session. Three structural issues are in-scope:

1. **Compute speed opportunities** that remain after the recent perf sweep (cache hygiene, AQE/Kryo defaults, `data.count()` removal, haversine consolidation).
2. **Dead code** — two fully commented-out files and several commented-out blocks inside live files that make the codebase harder to navigate.
3. **README drift** — install instructions, missing PyEntryPoint/update.sh/GCS documentation, a stale "place JAR in root directory" claim that no longer describes how the Python side consumes the artifact.

The Python⇄Scala JAR name is already consistent end-to-end (`sparkmobility-0.1.0.jar`) after today's fix to `pushToGCS.py`, so JAR wiring is out of scope.

Broader structural items (license mismatch — already fixed; camelCase object naming; CI scope; `SparkConfig.json` hardware assumptions; `runModeFromEnv` inverted logic; `PyEntryPoint` contract docs) live in [REPO_FOLLOWUPS.md](/data_1/albert/2_Mobility/sparkmobility-scala/REPO_FOLLOWUPS.md) and are deliberately left for separate PRs.

## Approach

Four independent commits, each landable on its own.

### Commit 0 — Persist this plan in the repo

Copy this plan file verbatim to `/data_1/albert/2_Mobility/sparkmobility-scala/SCALA_CLEANUP_PLAN.md` so it survives beyond the ephemeral `~/.claude/plans/` location and is visible alongside [REPO_FOLLOWUPS.md](/data_1/albert/2_Mobility/sparkmobility-scala/REPO_FOLLOWUPS.md). No other changes in this commit.

### Commit 1 — Perf: top 3 measurable wins

Picked from the survey: the ones with clear file:line and expected impact. Skipping speculative changes (#4 withColumn chains, #5 skew-threshold on collect_list) where the impact is <10% without profiling confirmation.

**1a. Use `H3CoreSingleton` in `h3Indexer`**
- File: [src/main/scala/sparkjobs/filtering/h3Indexer.scala:16-17](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/sparkjobs/filtering/h3Indexer.scala#L16-L17)
- Current: `H3Core.newInstance()` inside the UDF body — re-created per row.
- Change: reuse `utils.GeoDistance.H3CoreSingleton.instance` (already exists at [utils/GeoDistance.scala:21-23](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/utils/GeoDistance.scala#L21-L23); same pattern used by `extractTrips.scala:18-20`).
- Expected: 10–20% speedup on any pipeline that calls `addIndex`.

**1b. Broadcast `h3RegionMapping` in `mapToH3` join**
- File: [src/main/scala/pipelines/Pipelines.scala:171-172](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/pipelines/Pipelines.scala#L171-L172)
- Current: `stays.join(h3RegionMapping, Seq("caid", "h3_id"), "left")` — no broadcast hint; Spark defaults to sort-merge because `stays` is large.
- Change: wrap the right side in `broadcast(h3RegionMapping)` — `h3RegionMapping` is one row per `(caid, h3_id)` from `collect_list` + UDF, much smaller than `stays`.
- Expected: eliminates a wide shuffle on `stays`. 10–20% on the staydetection pipeline.
- Risk: if a user has extreme H3-cell diversity the broadcast side grows. AQE will downgrade to SMJ if it blows the broadcast threshold, so correctness is safe — worst case no speedup.

**1c. Defer sequential stay detection's RDD round-trip**
- File: [src/main/scala/sparkjobs/staydetection/StayDetection.scala:110-138, 400-408](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/sparkjobs/staydetection/StayDetection.scala#L110-L138)
- Current: `collect_list(struct(...))` → `.rdd.flatMap(...)` → manual `Map` construction → `arrays_zip` back into columns. Leaves Catalyst twice.
- Change: keep the groupBy + collect_list, but run the sequential stay detection as a typed `Dataset.flatMap[StayRow]` keyed by caid, returning a strongly-typed case class. Or — if the UDF body is worth keeping — wrap in a single `Dataset[(String, Array[Stay])]` operation and avoid the RDD hop.
- Expected: 15–25% on `getStays`. Biggest single win but largest diff.
- Defer-if-risky: if the existing logic has test coverage we can't regenerate, split out (1a)+(1b) first and land this in a follow-up.

### Commit 2 — Delete dead code

- Delete [src/test/scala/MySuite.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/test/scala/MySuite.scala) entirely (10 lines, all commented out). Note in REPO_FOLLOWUPS.md item 6 that real tests are the follow-up.
- Delete [src/main/scala/sparkjobs/filtering/SampleJob.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/sparkjobs/filtering/SampleJob.scala) entirely (42 lines, all commented out, hardcoded `/Users/chrisc/Downloads/...` path).
- In [Main.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/Main.scala):
  - Drop lines 14-24 (commented `repartitionParquet`).
  - Drop lines 31-36 and 40-41 (commented pipeline calls).
  - Keep the single `pipe.getFullODMatrix(...)` call, but move the hardcoded `/data_1/quadrant/...` paths into `args(0)`/`args(1)`/`args(2)` with a usage message. Flagged in REPO_FOLLOWUPS.md #2 — this is a natural time to fix it.
- In [Pipelines.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/pipelines/Pipelines.scala):
  - Drop lines 57-59 (commented `readCSVData`/sampling).
  - Drop lines 114-152 (39-line tmpGetStaysPath experiment, replaced by the current cache strategy).
  - Drop lines 186-189 (`exampleFunction` placeholder — grep first to confirm no callers; tests don't reference it).
  - Drop lines 219-222 (commented home-write block).
- In [build.sbt](/data_1/albert/2_Mobility/sparkmobility-scala/build.sbt):
  - Drop lines 23-24 (commented assembly options).

Git history preserves the tmpGetStaysPath experiment if anyone ever needs it.

### Commit 3 — README refresh

- **Line 27**: remove "Ideally, place it in the root directory of this project." The Python side downloads from GCS into `sparkmobility/lib/` via `ensure_jar()`; root-directory placement is not how callers consume the JAR.
- **Line 106** (`spark-submit` example): add a second example showing Python integration via `pipelines.PyEntryPoint` over py4j — the primary consumer today is the Python package, not direct `spark-submit`.
- **New "For Library Development" section** between current §"Changes on scala code" and §"Running the Project":
  - `./update.sh` as the standard dev loop (format → compile → assemble → push to GCS).
  - Note that `pushToGCS.py` requires `GOOGLE_APPLICATION_CREDENTIALS` to point at a key with write access to the `sparkmobility` bucket.
  - JAR version MUST match `sparkmobility/pyproject.toml` version; document the [build.sbt:3](/data_1/albert/2_Mobility/sparkmobility-scala/build.sbt#L3) ↔ `pyproject.toml` pairing to prevent the drift flagged in today's JAR audit.
- **New "Python integration" section** (short): one paragraph pointing at `pipelines.PyEntryPoint` as the only supported surface for Python callers, with a link to the Python package.
- **Authors section**: verify handles — touch nothing if already correct.

## Critical files to modify

Commit 1:
- [src/main/scala/sparkjobs/filtering/h3Indexer.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/sparkjobs/filtering/h3Indexer.scala)
- [src/main/scala/pipelines/Pipelines.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/pipelines/Pipelines.scala) (one broadcast hint)
- [src/main/scala/sparkjobs/staydetection/StayDetection.scala](/data_1/albert/2_Mobility/sparkmobility-scala/src/main/scala/sparkjobs/staydetection/StayDetection.scala) (RDD-round-trip rework; risky — gate on testability)

Commit 2:
- Delete: `src/test/scala/MySuite.scala`, `src/main/scala/sparkjobs/filtering/SampleJob.scala`
- Edit: `src/main/scala/Main.scala`, `src/main/scala/pipelines/Pipelines.scala`, `build.sbt`

Commit 3:
- [README.md](/data_1/albert/2_Mobility/sparkmobility-scala/README.md) — lines 27, 106, plus two new sections

## Verification

1. **Build + assembly still green.** `sbt clean compile assembly` produces `target/scala-2.13/sparkmobility-0.1.0.jar` without warnings introduced.
2. **Scalafmt/scalafix green.** `sbt scalafmtCheckAll "scalafixAll --check" headerCheck` — CI-equivalent checks pass.
3. **Python integration smoke test.** From a sparkmobility-testing conda shell:
   ```python
   from sparkmobility.processing import StayDetection
   # runs through py4j against the new JAR
   ```
   Compare stay counts on a small parquet input against a pre-change run — must match within the tolerance allowed by the RDD-rework change (commit 1c).
4. **Dead-file delete safety.** `rg -l "SampleJob|MySuite|exampleFunction" src/` returns empty after commit 2.
5. **README renders correctly.** GitHub preview or `grip` locally — code fences are balanced, links resolve.

## Out of scope

- camelCase → PascalCase object rename (REPO_FOLLOWUPS.md #3) — breaking change; separate PR.
- `runModeFromEnv` inverted logic (REPO_FOLLOWUPS.md #4) — needs original-intent review.
- CI branch scope expansion (REPO_FOLLOWUPS.md #5).
- Writing actual test coverage to replace `MySuite.scala` (REPO_FOLLOWUPS.md #6).
- `SparkConfig.json` hardware defaults (REPO_FOLLOWUPS.md #7).
- `PyEntryPoint` contract doc (REPO_FOLLOWUPS.md #8).
- Perf opportunities #4 (withColumn chain fusion) and #5 (collect_list skew threshold) from the Explore survey — <10% impact without profiler confirmation.
