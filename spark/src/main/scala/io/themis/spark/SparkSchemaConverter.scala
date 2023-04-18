package io.themis.spark

import io.themis.Table
import io.themis.column.{ArrayColumn, BigIntColumn, BinaryColumn, BooleanColumn, ByteColumn, CharColumn, ColumnVisitor, DateColumn, DecimalColumn, DoubleColumn, FloatColumn, IntColumn, MapColumn, ShortColumn, StringColumn, StructColumn, TimestampColumn, TimestampLocalTzColumn, TimestampTzColumn, VarcharColumn}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object SparkSchemaConverter extends ColumnVisitor[DataType] {
  override def visit(column: BooleanColumn): DataType = BooleanType

  override def visit(column: ByteColumn): DataType = ByteType

  override def visit(column: ShortColumn): DataType = ShortType

  override def visit(column: IntColumn): DataType = IntegerType

  override def visit(column: BigIntColumn): DataType = LongType

  override def visit(column: FloatColumn): DataType = FloatType

  override def visit(column: DoubleColumn): DataType = DoubleType

  override def visit(column: DecimalColumn): DataType = DecimalType(column.getPrecision, column.getScale)

  override def visit(column: StringColumn): DataType = StringType

  override def visit(column: CharColumn): DataType = StringType

  override def visit(column: VarcharColumn): DataType = StringType

  override def visit(column: BinaryColumn): DataType = BinaryType

  override def visit(column: DateColumn): DataType = DateType

  override def visit(column: TimestampColumn): DataType = {
    throw new UnsupportedOperationException()
  }

  override def visit(column: TimestampLocalTzColumn): DataType = {
    if (column.getPrecision != 6) {
      throw new UnsupportedOperationException()
    }

    TimestampType
  }

  override def visit(column: TimestampTzColumn): DataType = {
    throw new UnsupportedOperationException()
  }

  override def visit(column: ArrayColumn): DataType = ArrayType(
    column.getElement.accept(this),
    !column.required())

  override def visit(column: MapColumn): DataType = MapType(
    column.getKey.accept(this),
    column.getValue.accept(this),
    !column.getValue.required())

  override def visit(column: StructColumn): DataType = StructType(column.getFields.asScala.map {
    field => StructField(field.name, field.accept(this), !field.required())
  })

  def from(table: Table): StructType = {
    table.schema().accept(this).asInstanceOf[StructType]
  }
}
