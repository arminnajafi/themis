package io.themis.column;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Represents values comprising a set of key-value pairs.
 * Keys are not allowed to have null values.
 * valueContainsNull is used to indicate if values of a MapType value can have null values.
 */
public class MapColumn extends Column {

    private final Column key;
    private final Column value;

    public MapColumn(int id, String name, boolean required,
                     Column key, Column value) {
        super(id, name, String.format("map<%s,%s>", key.type(), value.type()), required);
        Preconditions.checkArgument(key.required(), "Map key must be required");
        this.key = key;
        this.value = value;
    }

    public Column getKey() {
        return key;
    }

    public Column getValue() {
        return value;
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MapColumn mapColumn = (MapColumn) o;
        return Objects.equals(key, mapColumn.key) &&
                Objects.equals(value, mapColumn.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, value);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MapColumn{");
        sb.append("key=").append(key);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
