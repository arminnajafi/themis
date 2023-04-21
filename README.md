![themis-logo](https://github.com/jackye1995/themis/blob/master/themis-logo.png?raw=true)

# Project Themis

Project Themis is an open benchmarking framework for big data solutions.

# Architecture

Themis consists of 5 layers:

## Layer 1: Spec

Themis spec define necessary component for benchmarking, such as generative dataset, benchmark workflow and reporting.
There are implementations of the spec in different languages such as Python and Java.

## Layer 2: Artifacts

Artifacts are materialized implementation of Themis spec that are required for benchmark execution,
such as a Jar for Spark data generation application or a text file for a set of SQL queries to benchmark against.

## Layer 3: CLI

A Python CLI is used to orchestrate each step in a benchmark execution.

## Layer 4: Airflow

Apache Airflow is used to define a complete benchmark workflow as a DAG,
and automate the orchestration by directly invoking the CLI to execute commands in the DAG.

## Layer 5: CDK

AWS CDK integration is provided for bootstrapping a Themis benchmark environment on AWS.

# Getting Started

Build shadow jar:

```shell
./gradlew :themis-spark-iceberg:shadowJar
```

Upload to S3:

```shell
aws s3 cp spark-iceberg/build/libs/themis-spark-iceberg-0.1.0-all.jar s3://yzhaoqin-iceberg-test/themis/themis-spark-iceberg-0.1.0-all.jar
```

Create EMR Spark cluster, make sure Glue for Spark and Hive catalog is enabled.
Then run EMR step using `command-runner.jar` with the following configs input:

```
spark-submit 
--class io.themis.spark.iceberg.IcebergDatagen
s3://yzhaoqin-iceberg-test/themis/themis-spark-iceberg-0.1.0-all.jar
--gen random 
--database-name themis_simple 
--database-location s3://yzhaoqin-iceberg-test/themis/themis_simple
--drop-database-if-exists 
--parallelism 100 
--row-count 10000
--replace-tables
```

You will see 3 tables corresponding to the ones defined in `io.themis.datagen.SimpleTables`