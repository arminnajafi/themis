package io.themis.column;

import java.util.Objects;

/**
 * TIMESTAMP WITH LOCAL TIME ZONE is another variant of TIMESTAMP.
 * It differs from TIMESTAMP WITH TIME ZONE as follows:
 * data stored in the database is normalized to the database time zone,
 * and the time zone offset is not stored as part of the column data.
 * When users retrieve the data, database returns it in the users' local session time zone.
 * The time zone offset is the difference (in hours and minutes) between local time and UTC.
 */
public class TimestampLocalTzColumn extends Column {

    private final int precision;

    public TimestampLocalTzColumn(int id, String name, boolean required, int precision) {
        super(
                id,
                name,
                String.format("timestamplocaltz(%d)", precision),
                required);
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
        TimestampLocalTzColumn that = (TimestampLocalTzColumn) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TimestampLocalTzColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append(", precision=").append(precision);
        sb.append('}');
        return sb.toString();
    }
}
