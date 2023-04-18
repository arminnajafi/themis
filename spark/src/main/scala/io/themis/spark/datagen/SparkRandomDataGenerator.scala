package io.themis.spark.datagen

import io.themis.column.{CharColumn, MapColumn, StringColumn, StructColumn, VarcharColumn}
import io.themis.datagen.RandomDataGenerator
import org.apache.spark.sql.Row

import java.util.Optional
import scala.collection.JavaConverters._

class SparkRandomDataGenerator(seed : Int) extends RandomDataGenerator(seed : Int) {

  override def visit(column: MapColumn): Object = {
    val data = super.visit(column)
    if (data == null) {
      return null
    }

    data.asInstanceOf[java.util.Map[Object, Optional[Object]]].asScala.map {
      case (key, value) => key -> value.orElse(null)
    }.toMap
  }

  override def visit(column: StructColumn): AnyRef = {
    val data = super.visit(column)
    if (data == null) {
      return null
    }

    Row.fromSeq(data.asInstanceOf[java.util.Map[String, Optional[Object]]].asScala.values.map {
      value => value.orElse(null)
    }.toSeq)
  }
}
