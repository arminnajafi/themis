package io.themis.column;

/**
 * Represents character string values without length limitation
 */
public class StringColumn extends Column {

    public StringColumn(int id, String name, boolean required) {
        super(id, name, "string", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StringColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
