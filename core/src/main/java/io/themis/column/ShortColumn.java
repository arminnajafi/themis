package io.themis.column;

/**
 * Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
 */
public class ShortColumn extends Column {

    public ShortColumn(int id, String name, boolean required) {
        super(id, name, "short", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ShortColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
