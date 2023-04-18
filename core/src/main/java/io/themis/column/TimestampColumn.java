package io.themis.column;

import java.util.Objects;

/**
 * The TIMESTAMP datatype is an extension of the DATE datatype.
 * It stores year, month, day, hour, minute, and second values.
 * It also stores fractional seconds, which are not stored by the DATE datatype.
 */
public class TimestampColumn extends Column {

    private final int precision;

    public TimestampColumn(int id, String name, boolean required, int precision) {
        super(id, name, String.format("timestamp(%d)", precision), required);
        this.precision = precision;
    }

    public int getPrecision() {
        return precision;
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
        TimestampColumn that = (TimestampColumn) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TimestampColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append(", precision=").append(precision);
        sb.append('}');
        return sb.toString();
    }
}
