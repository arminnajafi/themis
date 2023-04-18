package io.themis.column;

public interface ColumnVisitor<T> {

  T visit(BooleanColumn column);

  T visit(ByteColumn column);

  T visit(ShortColumn column);

  T visit(IntColumn column);

  T visit(BigIntColumn column);

  T visit(FloatColumn column);

  T visit(DoubleColumn column);

  T visit(DecimalColumn column);

  T visit(StringColumn column);

  T visit(CharColumn column);

  T visit(VarcharColumn column);

  T visit(BinaryColumn column);

  T visit(DateColumn column);

  T visit(TimestampColumn column);

  T visit(TimestampLocalTzColumn column);

  T visit(TimestampTzColumn column);

  T visit(ArrayColumn column);

  T visit(MapColumn column);

  T visit(StructColumn column);

}
