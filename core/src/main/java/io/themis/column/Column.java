package io.themis.column;

import java.io.Serializable;
import java.util.Objects;

public abstract class Column implements Serializable {

  private final int id;
  private final String name;
  private final String type;
  private final boolean required;

  Column(int id, String name, String type, boolean required) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.required = required;
  }

  public int id() {
    return id;
  }

  public String name() {
    return name;
  }

  public String type() {
    return type;
  }

  public boolean required() {
    return required;
  }

  public abstract <T> T accept(ColumnVisitor<T> visitor);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column column = (Column) o;
    return id == column.id && required == column.required && Objects.equals(name, column.name) && Objects.equals(type, column.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, type, required);
  }
}
