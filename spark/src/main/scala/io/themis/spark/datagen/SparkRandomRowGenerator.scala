package io.themis.spark.datagen

import io.themis.{Datasets, Table}
import io.themis.datagen.RandomRowGenerator
import io.themis.spark.{SparkRowGenerator, SparkSchemaConverter}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.Serializable
import scala.collection.JavaConverters._


class SparkRandomRowGenerator(spark: SparkSession, config: SparkRandomRowGeneratorConfig) extends SparkRowGenerator with Logging with Serializable {

  override def tables(): Seq[Table] = {
    val tables = Datasets.all.get(config.dataset).values.asScala.toSeq
    if (tables == null) {
      throw new IllegalArgumentException("Unknown variant, please use one of " +
        String.join(",", Datasets.all().keySet()))
    }

    if (config.filter != null) {
      val filter = config.filter.split(",")
      tables.filter(t => filter.contains(t.name))
    } else {
      tables
    }
  }

  override def createDF(table: Table): DataFrame = {
    val sparkContext: SparkContext = spark.sqlContext.sparkContext
    val rowCountPerWorker = config.rowCount / config.parallelism
    val data = sparkContext.parallelize(1 to config.parallelism, config.parallelism)
      // use the parallelism thread ID as the random seed to ensure each thread generates different rows
      .flatMap(i => new RandomRowGenerator(table, new SparkRandomDataGenerator(i), rowCountPerWorker).asScala)
      .map(values => Row.fromSeq(values))

    val schema = SparkSchemaConverter.from(table)
    data.setName(s"${table.name}, rowCount=${config.rowCount}")
    spark.sqlContext.createDataFrame(data, schema)
  }
}
