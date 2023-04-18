package io.themis.column;

import java.util.Objects;

/**
 * Represents arbitrary-precision signed decimal numbers.
 * Backed internally by java.math.BigDecimal.
 * A BigDecimal consists of an arbitrary precision integer unscaled value
 * and a 32-bit integer scale.
 */
public class DecimalColumn extends Column {

    private final int precision;
    private final int scale;

    public DecimalColumn(int id, String name, boolean required, int precision, int scale) {
        super(id, name, String.format("decimal(%d,%d)", precision, scale), required);
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
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
        DecimalColumn that = (DecimalColumn) o;
        return precision == that.precision &&
                scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BigIntColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append("precision=").append(precision);
        sb.append(", scale=").append(scale);
        sb.append('}');
        return sb.toString();
    }
}
