package io.themis.column;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents values with the structure described by a sequence of fields.
 */
public class StructColumn extends Column {

    private final List<Column> fields;

    public StructColumn(int id, String name, boolean required, List<Column> fields) {
        super(
                id,
                name,
                String.format("struct<%s>", fields.stream()
                        .map(f -> String.format("%s:%s", f.name(), f.type()))
                        .collect(Collectors.joining(","))),
                required);
        this.fields = fields;
    }

    public List<Column> getFields() {
        return fields;
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
        StructColumn that = (StructColumn) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StructColumn{");
        sb.append("fields=").append(fields);
        sb.append('}');
        return sb.toString();
    }
}
