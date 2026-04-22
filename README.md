![Scala CI](https://github.com/humnetlab/sparkmobility-scala/actions/workflows/scala.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![GitHub contributors](https://img.shields.io/github/contributors/humnetlab/sparkmobility-scala?cacheSeconds=3600)

# sparkmobility-scala

This is the Scala repository of `sparkmobility`. The Python user interface for `sparkmobility` can be found at [sparkmobility](https://github.com/humnetlab/sparkmobility).

## Pre-requisites
### Packaging the Scala Code

Whether modifying the Scala core of the project or running it, you must compile the Scala code and create a JAR file. Follow these steps:

1. **Download and Install SBT:**

    Install sbt on your machine. You can follow the instructions on the [official sbt documentation](https://www.scala-sbt.org/1.x/docs/Setup.html).

2. **Package the Scala Code:**

    Install,Compile and Package the Scala code using sbt:

    ```bash
    sbt update
    sbt compile
    sbt assembly
    ```
    This process creates a `target/scala-2.13/sparkmobility-<version>.jar`. The Python package downloads a matching artifact from the repo's GitHub Releases into `sparkmobility/lib/`, so you don't need to copy the JAR anywhere for the normal Python workflow. Note the path only if you intend to `spark-submit` the JAR directly.

## Setup and Installation of Spark

Sometimes, the default Scala version is 2.12. Since the project is compatible only with Scala 2.13 and above, you must set the Scala version before setting up the project. To do this, follow the instructions below:


1. **Download Spark:**
    - Go to the [Apache Spark download page](https://spark.apache.org/downloads.html).
    - Select the following options:
      - **Spark release:** 3.3.x or later
      - **Package type:** Pre-built for Apache Hadoop
      - **Scala version:** 2.13
    - Download the `.tgz` file.

2. **Extract the Spark Distribution:**
    Extract the downloaded file to a directory (e.g., `/opt/spark`):

    ```bash
    tar -xzf spark-3.5.4-bin-hadoop3-scala2.13.tgz -C /opt/spark
    ```

3. **Set Environment Variables:**
    Add the following environment variables to your shell configuration file (e.g., `.bashrc` or `.zshrc`):

    ```bash
    export SPARK_HOME=/opt/spark/spark-3.5.4-bin-hadoop3-scala2.13
    export PATH=$SPARK_HOME/bin:$PATH
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=python3
    ```

4. **Reload the Shell Configuration:**

    ```bash
    source ~/.bashrc  # or source ~/.zshrc
    ```

5. **Verify the Scala Version:**
    Run the following command to verify the Scala version:

     ```bash
     spark-shell --version
     ```
     
     This will display information including the Scala version. Ensure it shows `Scala version: 2.13.x`.


### Using the Project
This is a standard sbt project. You can use the following commands:

- Update dependencies: `sbt update`
- Compile the code: `sbt compile`
- Run the project: `sbt run`
- Start a Scala REPL: `sbt console`

### Changes on scala code

The scala code is located in the `src/main/scala` directory. You can modify the code in this directory.
Once edited you must follow the steps below to compile and package the code.

```
sbt compile
sbt assembly
```
This process creates a .jar file(Usually under target/scala-2.13/ dir) that should be submitted to the Spark cluster.

### For library development

The standard dev loop is `./update.sh`, which runs the scalafmt formatters, compiles, and builds the assembly JAR. To cut a new release:

1. Bump `ThisBuild / version` in [build.sbt](build.sbt) to match the sparkmobility Python package's `pyproject.toml`. The two versions MUST stay in lockstep — `ensure_jar()` in the Python package downloads `sparkmobility-<version>.jar` by string match.
2. Commit and push.
3. Tag and push the tag: `git tag v<version> && git push origin v<version>`.

The [`.github/workflows/release.yml`](.github/workflows/release.yml) workflow verifies that the tag matches `build.sbt`, builds the assembly, and attaches the JAR to a GitHub Release. Python's `ensure_jar()` pulls from that release on first import.

### Python integration

The Python package (`sparkmobility` on PyPI / [humnetlab/sparkmobility](https://github.com/humnetlab/sparkmobility)) calls into the JAR via py4j, using [`pipelines.PyEntryPoint`](src/main/scala/pipelines/PyEntryPoint.scala) as the only supported entry surface. Methods there take primitive args plus JSON strings — no Scala collections cross the bridge. Changing a `PyEntryPoint` signature is a breaking change for Python callers and requires a coordinated PR on both repos.

### Running the Project
To run the project, you can use the following command:

```bash

spark-submit \
  --class pipelines.Main \
  --master  <your_master_url> \ # e.g., local[*], yarn
  --driver-memory <MEMORY>g \
  --executor-memory <MEMORY>g \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootLogger=WARN,console" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.rootLogger=WARN,console" \
  /path/to/your/application.jar \
  <input-parquet> <output-dir> <h3-resolution>
```

`Main` is the OD-matrix entry point used for standalone `spark-submit`; the primary consumer today is the Python package via `PyEntryPoint` (see [Python integration](#python-integration)).

For more information on sbt, visit the [official sbt documentation](https://www.scala-sbt.org/1.x/docs/).

## License
This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Authors
- **Albert Cao** ([@caoalbert](https://github.com/caoalbert))
- **Christopher Chávez** ([@Vanchristoph3r](https://github.com/Vanchristoph3r))
- **Mingyi He** ([@Hemy17](https://github.com/Hemy17))
- **Wolin Jiang** ([@LowenJiang](https://github.com/LowenJiang))
- **Giuseppe Perona** ([@g-perona](https://github.com/g-perona))
- **Jiaman Wu** ([@charmainewu](https://github.com/charmainewu))
