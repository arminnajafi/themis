package io.themis.column;

/**
 * Represents byte sequence values.
 */
public class BinaryColumn extends Column {

    public BinaryColumn(int id, String name, boolean required) {
        super(id, name, "binary", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BinaryColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
