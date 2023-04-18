package io.themis.column;

/**
 * Represents 4-byte single-precision floating point numbers.
 */
public class FloatColumn extends Column {

    public FloatColumn(int id, String name, boolean required) {
        super(id, name, "float", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FloatColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
