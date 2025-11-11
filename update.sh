#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

echo "Step 0: Formatting Scala code with scalafmt"

sbt scalafmtSbt

sbt scalafmtAll

echo "Step 1: Compiling Scala code with sbt"
sbt compile

echo "Step 2: Assembling JAR with sbt"
sbt assembly

echo "Step 3: Running Python script to push to GCS"
python3 pushToGCS.py