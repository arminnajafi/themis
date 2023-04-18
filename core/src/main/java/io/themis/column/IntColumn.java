package io.themis.column;

/**
 * Represents 4-byte signed integer numbers.
 * The range of numbers is from -2147483648 to 2147483647.
 */
public class IntColumn extends Column {

    public IntColumn(int id, String name, boolean required) {
        super(id, name, "int", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IntColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
