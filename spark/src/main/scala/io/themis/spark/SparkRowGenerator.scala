package io.themis.spark

import io.themis.Table
import org.apache.spark.sql.DataFrame

trait SparkRowGenerator {

  def tables(): Seq[Table]

  def createDF(table: Table): DataFrame

}
