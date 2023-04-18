package io.themis.column;

/**
 * Represents 8-byte double-precision floating point numbers.
 */
public class DoubleColumn extends Column {

    public DoubleColumn(int id, String name, boolean required) {
        super(id, name, "double", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DoubleColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
