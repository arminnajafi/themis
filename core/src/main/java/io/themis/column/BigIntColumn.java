package io.themis.column;

/**
 * Represents 8-byte signed integer numbers.
 * The range of numbers is from -9223372036854775808 to 9223372036854775807.
 */
public class BigIntColumn extends Column {

    public BigIntColumn(int id, String name, boolean required) {
        super(id, name, "bigint", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BigIntColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
