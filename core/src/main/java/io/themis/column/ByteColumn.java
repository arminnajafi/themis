package io.themis.column;

/**
 * Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127
 */
public class ByteColumn extends Column {

    public ByteColumn(int id, String name, boolean required) {
        super(id, name, "byte", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ByteColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
