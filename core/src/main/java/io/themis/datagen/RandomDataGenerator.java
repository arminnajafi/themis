package io.themis.datagen;

import io.themis.column.ArrayColumn;
import io.themis.column.BigIntColumn;
import io.themis.column.CharColumn;
import io.themis.column.Column;
import io.themis.column.DecimalColumn;
import io.themis.column.DoubleColumn;
import io.themis.column.FloatColumn;
import io.themis.column.MapColumn;
import io.themis.column.ShortColumn;
import io.themis.column.StringColumn;
import io.themis.column.StructColumn;
import io.themis.column.TimestampColumn;
import io.themis.column.TimestampLocalTzColumn;
import io.themis.column.TimestampTzColumn;
import io.themis.column.VarcharColumn;
import io.themis.column.BinaryColumn;
import io.themis.column.BooleanColumn;
import io.themis.column.ByteColumn;
import io.themis.column.ColumnVisitor;
import io.themis.column.DateColumn;
import io.themis.column.IntColumn;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * Randomly generate
 */
public class RandomDataGenerator implements ColumnVisitor<Object> {

  private static final int ARRAY_SIZE_MIN = 1;
  private static final int ARRAY_SIZE_MAX = 5;
  private static final int MAP_SIZE_MIN = 1;
  private static final int MAP_SIZE_MAX = 5;
  private static final int STRING_LENGTH_MIN = 16;
  private static final int STRING_LENGTH_MAX = 4096;
  private static final int NULL_PERCENTAGE = 5;

  private final Random random;

  public RandomDataGenerator(long seed) {
    this.random = new Random(seed);
  }

  public RandomDataGenerator() {
    this.random = new Random();
  }

  @Override
  public Object visit(BooleanColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }
    return random.nextBoolean();
  }

  @Override
  public Object visit(ByteColumn column) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Object visit(ShortColumn column) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Object visit(IntColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return random.nextInt();
  }

  @Override
  public Object visit(BigIntColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return random.nextLong();
  }

  @Override
  public Object visit(FloatColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return random.nextFloat();
  }

  @Override
  public Object visit(DoubleColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return random.nextDouble();
  }

  @Override
  public Object visit(DecimalColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    int wholeDigits = column.getPrecision() - column.getScale();
    int whole = random.nextInt((int) Math.pow(10, wholeDigits));
    int fraction = random.nextInt((int) Math.pow(10, column.getScale()));
    return new BigDecimal(String.format("%d.%0" + column.getScale() + "d", whole, fraction));
  }

  @Override
  public Object visit(StringColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return generateRandomFixLengthString(random.nextInt(STRING_LENGTH_MAX - STRING_LENGTH_MIN)  + 1);
  }

  @Override
  public Object visit(CharColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return generateRandomFixLengthString(column.getLength());
  }

  @Override
  public Object visit(VarcharColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    return generateRandomFixLengthString(random.nextInt(column.getLength()) + 1);
  }

  @Override
  public Object visit(BinaryColumn column) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Object visit(DateColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    String value = String.format("%d-%02d-%02d",
        random.nextInt(20) + 1990,
        random.nextInt(12) + 1,
        random.nextInt(28) + 1);
    return Date.valueOf(value);
  }

  @Override
  public Object visit(TimestampColumn column) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Object visit(TimestampLocalTzColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }
    // e.g. 2000-01-01 01:01:01.111
    String value = String.format("%d-%02d-%02d %02d:%02d:%02d.%0" + column.getPrecision() + "d",
            random.nextInt(20) + 1990,
            random.nextInt(12) + 1,
            random.nextInt(28) + 1,
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60),
            random.nextInt((int) Math.pow(10, column.getPrecision())));
    return Timestamp.valueOf(value);
  }

  @Override
  public Object visit(TimestampTzColumn column) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Object visit(ArrayColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    int size = random.nextInt(ARRAY_SIZE_MAX - ARRAY_SIZE_MIN) + ARRAY_SIZE_MIN;
    Object[] array = new Object[size];
    for (int i = 0; i < size; i++) {
      array[i] = column.getElement().accept(this);
    }

    return array;
  }

  @Override
  public Object visit(MapColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    Map<Object, Optional<Object>> map = new HashMap<>();
    int size = random.nextInt(MAP_SIZE_MAX - MAP_SIZE_MIN)  + 1;
    for (int i = 0; i < size; i++) {
      map.put(column.getKey().accept(this),
              Optional.ofNullable(column.getValue().accept(this)));
    }

    return map;
  }

  @Override
  public Object visit(StructColumn column) {
    if (shouldReturnNull(column)) {
      return null;
    }

    Map<String, Optional<Object>> struct = new HashMap<>();
    for (Column subColumn : column.getFields()) {
      struct.put(subColumn.name(), Optional.ofNullable(subColumn.accept(this)));
    }

    return struct;
  }

  private String generateRandomFixLengthString(int length) {
    return random.ints(48, 123) // 0 to z
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
  }

  private boolean shouldReturnNull(Column column) {
    if (column.required()) {
      return false;
    }

    return random.nextInt(100) < NULL_PERCENTAGE;
  }

}
