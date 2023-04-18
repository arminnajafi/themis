package io.themis.column;

import java.util.Objects;

/**
 * A variant of VarcharType(length) which is fixed length.
 * Reading column of type CharType(n) always returns string values of length n.
 * Char type column comparison will pad the short one to the longer length.
 */
public class CharColumn extends Column {

    private final int length;

    public CharColumn(int id, String name, boolean required, int length) {
        super(id, name, String.format("char(%d)", length) , required);
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
        CharColumn that = (CharColumn) o;
        return length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), length);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CharColumn{");
        sb.append("id=").append(id());
        sb.append(", name='").append(name()).append('\'');
        sb.append(", type='").append(type()).append('\'');
        sb.append(", required=").append(required());
        sb.append(", length=").append(length);
        sb.append('}');
        return sb.toString();
    }
}
