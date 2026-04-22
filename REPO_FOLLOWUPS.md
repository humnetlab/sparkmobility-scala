# sparkmobility-scala repository follow-ups

Structural cleanup across the Scala repo. Ordered by ROI. Items 1–3 are
already fixed in this branch; the rest are proposed separately.

## Already fixed

- **README license mismatch.** Claimed MIT; LICENSE file and `headerLicense`
  in [build.sbt](build.sbt#L12-L14) are Apache-2.0. Source files already
  carry ALv2 SPDX headers. Fixed [README.md:112](README.md#L112).
- **Stale project name.** [build.sbt:26](build.sbt#L26) declared
  `name := "timegeo_1"` — a vestige of the timegeo-only era. Changed to
  `"sparkmobility"` to match the repo and `assemblyJarName` prefix. The
  assembly output name is unchanged (`sparkmobility-0.1.0.jar`) because
  `assembly / assemblyJarName` still overrides it.
- **Broken `pushToGCS.py`.** Referenced `target/scala-2.13/timegeo010.jar`
  (doesn't exist — actual output is `sparkmobility-0.1.0.jar`) and
  uploaded as blob `sparkmobility010.jar`, while the Python package's
  `ensure_jar()` in [sparkmobility/__init__.py:21](../../sparkmobility/sparkmobility/__init__.py#L21)
  downloads `sparkmobility-{version}.jar`. That mismatch meant every
  `pip install sparkmobility` ran against a stale JAR — or 404'd. Script
  now version-parameterizes both sides and reads
  `GOOGLE_APPLICATION_CREDENTIALS` from env with a dev-box fallback.

## 1. Delete the commented-out block in `Pipelines.scala`

[Pipelines.scala:110-149](src/main/scala/pipelines/Pipelines.scala#L110-L149)
is 40 lines of a tmpGetStaysPath experiment left as a comment. The
current implementation above it is what actually runs. Git history has
the experiment if anyone needs it — dead comments just rot.

Same file: [Pipelines.scala:57-59](src/main/scala/pipelines/Pipelines.scala#L57-L59)
and [Pipelines.scala:219-222](src/main/scala/pipelines/Pipelines.scala#L219-L222)
(smaller commented `FileUtils.readCSVData` / home-write snippets).

## 2. Move hardcoded paths out of `Main.scala`

[Main.scala:37-42](src/main/scala/Main.scala#L37-L42) hardcodes
`/data_1/quadrant/output/...` — a user-specific path committed into
what's supposed to be a library's default entry point. Anyone else who
runs `spark-submit` on the JAR gets a `FileNotFoundException`.

Replace with `args(0)`/`args(1)`/`args(2)` (input, output, resolution)
and a usage message. Keep the existing path as an example in README.

Also delete the 10-line commented `repartitionParquet` block at
[Main.scala:14-24](src/main/scala/Main.scala#L14-L24).

## 3. Object naming convention (camelCase → PascalCase)

Scala convention is PascalCase for `object` and `class` declarations.
The repo is inconsistent:

- PascalCase (correct): `StayDetection`, `FileUtils`, `GeoDistance`,
  `SparkFactory`, `TestUtils`, `Pipelines`, `PyEntryPoint`, `SampleJob`,
  `Main`.
- lowercase (non-standard): `dataLoadFilter`, `h3Indexer`,
  `filterParameters`, `locationType`, `extractTrips`,
  `dailyVisitedLocation`, `departureTimeDistribution`,
  `locationDistribution`, `stayDurationDistribution`.

Breaking change — callers in Python `PyEntryPoint` (internal) and in
[Pipelines.scala](src/main/scala/pipelines/Pipelines.scala) (internal)
need the updates too. Do it in one PR across the repo while user count
is low.

## 4. The `runModeFromEnv` logic is inverted

[TestUtils.scala:13-23](src/main/scala/utils/TestUtils.scala#L13-L23):

```scala
val TESTSYSTEMS_RE = "false"
if (System.getenv("UNIT_TEST") == TESTSYSTEMS_RE) {
  RunMode.UNIT_TEST          // sets UNIT_TEST when env var is "false"??
} else {
  RunMode.PRODUCTION
}
```

This says: run in UNIT_TEST mode when `UNIT_TEST=false` is set. That's
either a bug or so cryptic it might as well be one. The `TESTSYSTEMS_RE`
name hints at a regex that got lost in translation.

Proposed semantics: `UNIT_TEST=1` (or `true`) → UNIT_TEST, anything
else → PRODUCTION. Needs a deliberate review — not touching in this PR
since nothing in git history documents the original intent.

## 5. CI only runs on `main`

[.github/workflows/scala.yml:3-6](.github/workflows/scala.yml#L3-L6)
triggers only on push/PR to `main`. Current work lives on
`improvements/phase-0-to-2-non-integration` — zero CI coverage for the
last ~10 commits.

Two fixes:
- Expand trigger: `branches: [ main, "improvements/**" ]` (or `"**"` to
  catch any branch) for push; keep PR branches unrestricted.
- Drop the Java 8 matrix entry; Spark 3.5 is the target and project
  compiles fine on 11/17. The 8 row is adding CI time for a JDK that's
  not the prod target.

## 6. The test suite is empty

[MySuite.scala](src/test/scala/MySuite.scala) is entirely commented
out. `sbt test` passes vacuously. CI green means nothing.

Minimum viable coverage — one file per the testable utility:

- `test/scala/utils/GeoDistanceSpec.scala` — haversine against a handful
  of known distances (easy, pure function, high value after the recent
  consolidation in commit `29ce0bf`).
- `test/scala/sparkjobs/staydetection/StayDetectionSpec.scala` — tiny
  synthetic DataFrame, assert stay count and H3 IDs for a known input.
  Commit `485fbfb` ("fix StayDetection correctness") is the kind of
  regression this would catch.
- `test/scala/pipelines/PyEntryPointSpec.scala` — round-trip a
  FilterParameters JSON through `fromJsonString` and back.

munit is already wired via `plugins.sbt`. Delete `MySuite.scala` once
the first real suite lands.

## 7. `SparkConfig.json` ships production-specific hardware assumptions

[SparkConfig.json:2](src/main/resources/config/SparkConfig.json#L2):
`spark.master` = `local[14]`, driver/executor memory 42g, 14 cores, 18g
off-heap. These are tuned for a specific 48-core dev box. Anyone who
picks up the repo and runs `sbt run` on a laptop gets OOM.

Options:
- A) Ship conservative defaults (`local[4]`, 4g) and document tuning in
  README. Users override via `spark-submit --conf` for production runs.
- B) Split into `SparkConfig.default.json` (safe) and
  `SparkConfig.humnetlab.json` (tuned), select via env var.

(A) is simpler.

## 8. Document the Python ↔ Scala contract

`PyEntryPoint` is the only supported surface for Python callers (per
its docstring), but nothing in the Python package says "only call these
methods, not `Pipelines` directly." Add a `docs/py4j-contract.md` (or
extend README) listing:

- The 7 methods `PyEntryPoint` exposes
- Their argument types (all primitive + JSON strings — no Scala `Map`
  handoffs)
- That breaking any of these signatures requires a coordinated PR to
  the Python repo's caller

This prevents a Scala refactor from silently breaking Python callers.

## Out of scope

- Publishing the JAR via sbt-ci-release or GitHub Releases (currently
  manual via `pushToGCS.py`).
- Splitting the repo into submodules (measures, pipelines, utils) — not
  worth the build complexity at current size.
- Moving from sbt-assembly to sbt-pack — assembly works fine for the
  single fat-JAR Python depends on.
