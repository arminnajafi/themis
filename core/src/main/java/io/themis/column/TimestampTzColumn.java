package io.themis.column;

import java.util.Objects;

/**
 * TIMESTAMP WITH TIME ZONE is a variant of TIMESTAMP that includes a time zone offset or
 * time zone region name in its value.
 * The time zone offset is the difference (in hours and minutes) between local time and UTC.
 */
public class TimestampTzColumn extends Column {

    private final int precision;

    public TimestampTzColumn(int id, String name, boolean required, int precision) {
        super(id, name, String.format("timestamptz(%d)", precision), required);
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
        TimestampTzColumn that = (TimestampTzColumn) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TimestampTzColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append(", precision=").append(precision);
        sb.append('}');
        return sb.toString();
    }
}
