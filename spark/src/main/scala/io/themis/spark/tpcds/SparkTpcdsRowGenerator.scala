package io.themis.spark.tpcds

import io.themis.Table
import io.themis.spark.SparkRowGenerator
import io.themis.tpcds.{TpcdsStringRowGenerator, TpcdsTables}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, rpad}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.Serializable
import scala.collection.JavaConverters._


class SparkTpcdsRowGenerator(spark: SparkSession, config: SparkTpcdsRowGeneratorConfig) extends SparkRowGenerator with Logging with Serializable {

  override def tables(): Seq[Table] = {
    val tables = config.variant match {
      case "default" => TpcdsTables.TABLES.values().asScala.toSeq
      case "unpartitioned" => TpcdsTables.UNPARTITIONED_TABLES.values().asScala.toSeq
      case _ => throw new IllegalArgumentException("Unsupported variant: " + config.variant)
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

    val stringDataRdd = sparkContext.parallelize(1 to config.parallelism, config.parallelism)
      .flatMap(i => new TpcdsStringRowGenerator(table, config.scaleFactor, config.parallelism, i).asScala)
      .map(values => Row.fromSeq(values.toSeq))

    var schema = StructType.fromDDL(table.ddlSchemaPart)
    val stringSchema = new StructType(schema.fields.map(f => StructField(f.name, StringType)))

    if (config.useStringForChar) {
      val newFields = schema.fields.map { field =>
        val newDataType = field.dataType match {
          case _: CharType | _: VarcharType => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }
      schema = StructType(newFields)
    }

    if (config.useDoubleForDecimal) {
      val newFields = schema.fields.map { field =>
        val newDataType = field.dataType match {
          case _: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }
      schema = StructType(newFields)
    }

    stringDataRdd.setName(s"${table.name}, sf=${config.scaleFactor}, strings")
    var data = spark.sqlContext.createDataFrame(stringDataRdd, stringSchema)

    // pad CHAR type data strings to specified length
    val columns = schema.fields.map { f =>
      val expr = f.dataType match {
        case CharType(n) => rpad(col(f.name), n, " ")
        case _ => col(f.name).cast(f.dataType)
      }
      expr.as(f.name)
    }
    data = data.select(columns: _*)

    if (config.cacheDF) {
      data = data.cache()
      // Trigger a count action and cache the dataframe
      // to generate data with whatever parallelism factor specified
      logInfo(s"${table.name} row count: ${data.count()}")
    }

    if (config.filterOutNullPartitionValues) {
      data = data.filter(row => table.partitionKeyColumns.values.asScala
        .map(col => row.get(schema.fieldIndex(col.name)) != null)
        .forall(_ == true))
    }

    if (config.repartitionTables) {
      if (table.partitionKeys.isEmpty) {
        // write only 1 file for unpartitioned table
        data = data.repartition(1)
      } else {
        val values = table.partitionKeyColumns.values.asScala.map(c => col(c.name)).toSeq
        data = data.repartition(values: _*)
      }
    }

    data
  }
}
