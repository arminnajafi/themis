package io.themis.datagen;

import io.themis.column.ArrayColumn;
import io.themis.column.TimestampColumn;
import io.themis.column.VarcharColumn;
import io.themis.column.ByteColumn;
import io.themis.column.CharColumn;
import io.themis.column.ColumnVisitor;
import io.themis.column.BigIntColumn;
import io.themis.column.BinaryColumn;
import io.themis.column.BooleanColumn;
import io.themis.column.Column;
import io.themis.column.DateColumn;
import io.themis.column.DecimalColumn;
import io.themis.column.DoubleColumn;
import io.themis.column.FloatColumn;
import io.themis.column.IntColumn;
import io.themis.column.MapColumn;
import io.themis.column.ShortColumn;
import io.themis.column.StringColumn;
import io.themis.column.StructColumn;
import io.themis.column.TimestampLocalTzColumn;
import io.themis.column.TimestampTzColumn;

import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Randomly generate
 */
public class PrestoRandomInsertGenerator implements ColumnVisitor<String> {

  // number of elements for each array generated
  private static final int ARRAY_SIZE = 2;

  // number of key-value pairs for each map generated
  private static final int MAP_SIZE = 2;

  private final Random random;
  private final TimeZone timeZone;

  public PrestoRandomInsertGenerator(TimeZone timeZone) {
    this.random = new Random();
    this.timeZone = timeZone;
  }

  public PrestoRandomInsertGenerator(long seed, TimeZone timeZone) {
    this.random = new Random(seed);
    this.timeZone = timeZone;
  }

  @Override
  public String visit(BooleanColumn column) {
    return Boolean.toString(random.nextBoolean());
  }

  @Override
  public String visit(ByteColumn column) {
    throw new UnsupportedOperationException("Byte type is not supported in Presto");
  }

  @Override
  public String visit(ShortColumn column) {
    throw new UnsupportedOperationException("Short type is not supported in Presto");
  }

  @Override
  public String visit(IntColumn column) {
    return Integer.toString(random.nextInt());
  }

  @Override
  public String visit(BigIntColumn column) {
    return Long.toString(random.nextLong());
  }

  @Override
  public String visit(FloatColumn column) {
    String val = Float.toString(random.nextFloat());
    return String.format("cast(%s as real)", val);
  }

  @Override
  public String visit(DoubleColumn column) {
    return Double.toString(random.nextDouble());
  }

  @Override
  public String visit(DecimalColumn column) {
    int wholeDigits = column.getPrecision() - column.getScale();
    int whole = random.nextInt((int) Math.pow(10, wholeDigits));
    int fraction = random.nextInt((int) Math.pow(10, column.getScale()));
    String format = "%d.%0" + column.getScale() + "d";
    return String.format("decimal '" + format + "'", whole, fraction);
  }

  @Override
  public String visit(StringColumn column) {
    String data = UUID.randomUUID().toString();
    return String.format("'%s'", data);
  }

  @Override
  public String visit(CharColumn column) {
    String data = UUID.randomUUID().toString();
    return String.format("cast('%s' as char(%d))", data, column.getLength());
  }

  @Override
  public String visit(VarcharColumn column) {
    String data = UUID.randomUUID().toString();
    return String.format("cast('%s' as varchar(%d))", data, column.getLength());
  }

  @Override
  public String visit(BinaryColumn column) {
    String data = UUID.randomUUID().toString();
    return String.format("varbinary '%s'", data);
  }

  @Override
  public String visit(DateColumn column) {
    String date = String.format("%d-%02d-%02d",
        random.nextInt(20) + 1990,
        random.nextInt(12) + 1,
        random.nextInt(28) + 1);
    return String.format("date '%s'", date);
  }

  @Override
  public String visit(TimestampColumn column) {
    throw new UnsupportedOperationException(
            "Timestamp with time zone type is not supported in Presto, " +
            "Presto timestamp is a timestamp with loca time zone in ABS");
  }

  @Override
  public String visit(TimestampLocalTzColumn column) {
    if (column.getPrecision() != 3) {
      throw new UnsupportedOperationException("Presto only supports timestamp with precision 3");
    }
    // e.g. 2000-01-01 01:01:01.111
    String timestamp = String.format("%d-%02d-%02d %02d:%02d:%02d.%03d",
            random.nextInt(20) + 1990,
            random.nextInt(12) + 1,
            random.nextInt(28) + 1,
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60),
            random.nextInt(1000));
    return String.format("timestamp '%s'", timestamp);
  }

  @Override
  public String visit(TimestampTzColumn column) {
    throw new UnsupportedOperationException("Timestamp with time zone type is not supported in Presto");
  }

  @Override
  public String visit(ArrayColumn column) {
    StringBuilder sb = new StringBuilder().append("array[");
    for (int i = 0; i < ARRAY_SIZE; i++) {
      sb.append(column.getElement().accept(this)).append(',');
    }

    sb.deleteCharAt(sb.length() - 1).append(']');
    return sb.toString();
  }

  @Override
  public String visit(MapColumn column) {
    StringBuilder keySb = new StringBuilder("array[");
    StringBuilder valueSb = new StringBuilder("array[");
    for (int i = 0; i < MAP_SIZE; i++) {
      keySb.append(column.getKey().accept(this)).append(',');
      valueSb.append(column.getValue().accept(this)).append(',');
    }

    keySb.deleteCharAt(keySb.length() - 1).append(']')
            .insert(0, "map(")
            .append(',')
            .append(valueSb)
            .deleteCharAt(keySb.length() - 1)
            .append(']')
            .append(')');
    return keySb.toString();
  }

  @Override
  public String visit(StructColumn column) {
    StringBuilder sb = new StringBuilder("row(");
    for (Column subColumn : column.getFields()) {
      String subValue = subColumn.accept(this);
      sb.append(subValue).append(',');
    }

    sb.deleteCharAt(sb.length() - 1).append(')');
    return sb.toString();
  }

}
