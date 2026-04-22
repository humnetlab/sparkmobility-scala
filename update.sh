#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

echo "Step 0: Formatting Scala code with scalafmt"

sbt scalafmtSbt

sbt scalafmtAll

echo "Step 1: Compiling Scala code with sbt"
sbt compile

echo "Step 2: Assembling JAR with sbt"
sbt assembly

# To publish: bump ThisBuild / version in build.sbt, commit, then:
#     git tag v<version> && git push origin v<version>
# .github/workflows/release.yml builds and attaches the JAR to the release.