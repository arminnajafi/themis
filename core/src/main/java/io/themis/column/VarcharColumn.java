package io.themis.column;

import java.util.Objects;

/**
 * A variant of string type which has a length limitation.
 */
public class VarcharColumn extends Column {

    private final int length;

    public VarcharColumn(int id, String name, boolean required, int length) {
        super(id, name, String.format("varchar(%d)", length), required);
        this.length = length;
    }

    public int getLength() {
        return length;
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
        VarcharColumn that = (VarcharColumn) o;
        return length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), length);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("VarcharColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append(", length=").append(length);
        sb.append('}');
        return sb.toString();
    }
}
