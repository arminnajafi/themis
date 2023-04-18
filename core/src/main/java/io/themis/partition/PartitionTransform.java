package io.themis.partition;

import io.themis.column.Column;

import java.io.Serializable;
import java.util.Objects;

public abstract class PartitionTransform implements Serializable {

  private final int columnId;
  private final String signature;

  public PartitionTransform(int columnId, String signature) {
    this.columnId = columnId;
    this.signature = signature;
  }

  public int columnId() {
    return columnId;
  }

  public String signature() {
    return signature;
  }

  public String partitionTransformName(Column column) {
    return String.format("%s(%s)", signature, column);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionTransform that = (PartitionTransform) o;
    return columnId == that.columnId && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnId, signature);
  }
}
