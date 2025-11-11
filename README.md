![Scala CI](https://github.com/humnetlab/sparkmobility-scala/actions/workflows/scala.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## sparkmobility-scala

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
    This process creates a `.jar` file that should be submitted to the Spark cluster. Ideally, place it in the root directory of this project. You will need the PATH to this jar in the next steps.

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

### Running the Project
To run the project, you can use the following command:

```bash

spark-submit \
  --class com.timegeo.Main \
  --master  <your_master_url> \ # e.g., local[*], yarn
  --driver-memory <MEMORY>g \
  --executor-memory <MEMORY>g \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.rootLogger=WARN,console" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.rootLogger=WARN,console" \
  /path/to/your/application.jar
  ```

For more information on sbt, visit the [official sbt documentation](https://www.scala-sbt.org/1.x/docs/).

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Authors
- **Albert Cao** ([@caoalbert](https://github.com/caoalbert))
- **Christopher Chávez** ([@Vanchristoph3r](https://github.com/Vanchristoph3r))
- **Mingyi He** ([@Hemy17](https://github.com/Hemy17))
- **Wolin Jiang** ([@LowenJiang](https://github.com/LowenJiang))
- **Giuseppe Perona** ([@g-perona](https://github.com/g-perona))
- **Jiaman Wu** ([@charmainewu](https://github.com/charmainewu))
