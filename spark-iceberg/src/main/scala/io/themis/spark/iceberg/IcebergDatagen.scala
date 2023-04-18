package io.themis.spark.iceberg

import io.themis.Table
import io.themis.partition.{BucketTransform, DayTransform, HourTransform, MonthTransform, TruncateTransform, YearTransform}
import io.themis.spark.SparkDatabaseConfig
import io.themis.spark.datagen.{SparkRandomRowGenerator, SparkRandomRowGeneratorConfig}
import io.themis.spark.tpcds.{SparkTpcdsRowGenerator, SparkTpcdsRowGeneratorConfig}
import org.apache.iceberg.TableProperties
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{bucket, col, days, hours, months, years}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object IcebergDatagen extends Logging {

  def main(args: Array[String]): Unit = {
    doMain(args)
  }

  // return a Spark session for further validation in tests
  def doMain(args: Array[String]): SparkSession = {
    val icebergConfig = new IcebergDatagenConfig(args)
    val databaseConfig = new SparkDatabaseConfig(args)
    val datagenConfig = icebergConfig.gen match {
      case "tpcds" => new SparkTpcdsRowGeneratorConfig(args)
      case "random" => new SparkRandomRowGeneratorConfig(args)
      case _ => throw new IllegalArgumentException("Unsupported generator: " + icebergConfig.gen)
    }

    val builder = SparkSession.builder.appName("IcebergDatagen")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")

    if (icebergConfig.local) {
      builder.master("local[2]")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        // not used, required by catalogs for write
        .config("spark.sql.catalog.iceberg.warehouse", databaseConfig.databaseLocation)
    } else {
      // TODO: support other catalogs
      builder.config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    }

    val spark = builder.getOrCreate()
    databaseConfig.ensureDatabaseExists(spark, "iceberg")

    val gen = icebergConfig.gen match {
      case "tpcds" => new SparkTpcdsRowGenerator(spark, datagenConfig.asInstanceOf[SparkTpcdsRowGeneratorConfig])
      case "random" => new SparkRandomRowGenerator(spark, datagenConfig.asInstanceOf[SparkRandomRowGeneratorConfig])
      case _ => throw new IllegalArgumentException("Unsupported generator: " + icebergConfig.gen)
    }

    val fileFormat = icebergConfig.fileFormat.toLowerCase
    val compressionPropertyKey: String = fileFormat match {
      case "parquet" => TableProperties.PARQUET_COMPRESSION
      case "orc" => TableProperties.ORC_COMPRESSION
      case "avro" => TableProperties.AVRO_COMPRESSION
      case _ => throw new IllegalArgumentException("Unsupported file format " + icebergConfig.fileFormat)
    }

    for (table : Table <- gen.tables()) {
      val fullTableName = s"iceberg.${databaseConfig.databaseName}.${table.name}"
      logInfo(s"Start generating data for table $fullTableName")
      val writer = gen.createDF(table)
        .writeTo(fullTableName)
        .using("iceberg")
        .tableProperty(TableProperties.FORMAT_VERSION, "2")
        .tableProperty(TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true")
        .tableProperty(TableProperties.OBJECT_STORE_ENABLED, icebergConfig.objectStorageMode.toString)
        .tableProperty(TableProperties.DEFAULT_FILE_FORMAT, fileFormat)
        .tableProperty(compressionPropertyKey, icebergConfig.compressionCodec.toLowerCase)

      val partitionKeys = table.partitionKeyColumns.asScala.toSeq.map {
        entry => entry._1 match {
          case _: BucketTransform => bucket(entry._1.asInstanceOf[BucketTransform].getBucket, col(entry._2.name()))
          // TODO: what's the function for truncate? tunc?
          // case _: TruncateTransform => (entry._1.asInstanceOf[TruncateTransform].getWidth, )
          case _: HourTransform => hours(col(entry._2.name()))
          case _: DayTransform => days(col(entry._2.name()))
          case _: MonthTransform => months(col(entry._2.name()))
          case _: YearTransform => years(col(entry._2.name()))
          case _ => col(entry._2.name())
        }
      }
      if (partitionKeys.nonEmpty) {
        writer.partitionedBy(partitionKeys.head, partitionKeys.drop(1): _*)
      }

      if (icebergConfig.replaceTable) {
        writer.createOrReplace()
      } else {
        writer.create()
      }
    }
    spark
  }
}
