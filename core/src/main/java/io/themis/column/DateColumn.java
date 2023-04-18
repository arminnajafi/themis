package io.themis.column;

/**
 * Represents values comprising values of fields year, month and day, without a time-zone.
 */
public class DateColumn extends Column {

    public DateColumn(int id, String name, boolean required) {
        super(id, name, "date", required);
    }

    @Override
    public <T> T accept(ColumnVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DateColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append('}');
        return sb.toString();
    }
}
