# Python ↔ Scala (py4j) contract

`pipelines.PyEntryPoint` is the **only** surface the Python `sparkmobility`
package is allowed to call. Everything under `pipelines.Pipelines`,
`sparkjobs.*`, and `utils.*` is internal — Python callers must not reach in.

If you change a signature in this file, you must also ship a coordinated PR to
the Python repo's caller (`sparkmobility/processing/*`). Python goes through
`spark._jvm.pipelines.PyEntryPoint`, so a silent rename will fail at runtime,
not at build time.

## Design rules

1. **Primitives and JSON strings only** across the boundary. No Scala `Map`,
   no `Seq`, no case classes in method signatures. Python should never have to
   call `PythonUtils.toScalaMap` or hand-build a Java collection.
2. **Config is a single JSON string** (`configJson`) deserialized inside Scala
   via `FilterParameters.fromJsonString`. The field list lives in
   [FilterParameters.scala](../src/main/scala/sparkjobs/filtering/FilterParameters.scala)
   and is covered by [PyEntryPointSpec](../src/test/scala/pipelines/PyEntryPointSpec.scala).
3. **Column-name maps are JSON objects** (`columnNamesJson`), parsed via
   `parseColumnMap`. This keeps the Python side to `json.dumps({...})`.
4. **Return types are `Unit`** — every method writes its result to
   `outputPath`. No large objects cross the bridge.

## Supported methods

| Method | Arguments | Purpose |
| --- | --- | --- |
| `getStays` | `inputPath: String`, `outputPath: String`, `timeFormat: String`, `inputFormat: String`, `delim: String`, `ifHeader: String`, `columnNamesJson: String`, `configJson: String` | Stay detection from raw pings |
| `getHomeWorkLocation` | `inputPath: String`, `outputPath: String`, `configJson: String` | Home/work inference from stays |
| `getODMatrix` | `inputPath: String`, `outputPath: String`, `hexResolution: Int` | Home→work OD matrix at H3 resolution |
| `getFullODMatrix` | `inputPath: String`, `outputPath: String`, `hexResolution: Int` | Full OD matrix across all stays |
| `getDailyVisitedLocation` | `inputPath: String`, `outputPath: String` | Daily visited-location counts |
| `getLocationDistribution` | `inputPath: String`, `outputPath: String` | Stay-location frequency distribution |
| `getStayDurationDistribution` | `inputPath: String`, `outputPath: String` | Stay-duration histogram |
| `getDepartureTimeDistribution` | `inputPath: String`, `outputPath: String` | Departure-time histogram |

## Tests that guard this contract

- [PyEntryPointSpec](../src/test/scala/pipelines/PyEntryPointSpec.scala)
  - Parses a representative `configJson` through `FilterParameters.fromJsonString`
    — any renamed field fails here.
  - Reflection-checks that `PyEntryPoint` exposes all eight method names above
    — any rename fails here before Python ever runs.

## Checklist before changing a signature

1. Update the method in `PyEntryPoint.scala`.
2. Update the expected-methods set in `PyEntryPointSpec.scala`.
3. Open a paired PR in the Python repo updating the caller.
4. Bump the JAR version in [build.sbt](../build.sbt) **and** the Python
   package's `pyproject.toml` in lockstep, so `ensure_jar()` pulls a matching
   artifact from the GitHub Release published on tag push.
