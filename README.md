# Project Themis

Project Themis is an open benchmarking framework for big data solutions.

## Getting Started

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