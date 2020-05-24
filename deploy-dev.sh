#!/usr/bin/env bash
export DIRECTORY=/tmp/spark-temp
export REBUILD=$1
export SCALA_VERSION=2.11

if [ "$REBUILD" -eq  "1" ]; then
    sbt clean build
fi

if [ ! -d "$DIRECTORY" ]; then
    mkdir $DIRECTORY
fi

./spark/bin/spark-submit \
         --class Main \
         --master local[*] \
         --conf executor.memory=4g \
         --conf spark.local.dir=/tmp/spark-temp \
         target/scala-${SCALA_VERSION}/*.jar
