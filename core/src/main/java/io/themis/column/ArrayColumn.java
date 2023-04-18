package io.themis.column;

import java.util.Objects;

/**
 * Represents values comprising a sequence of elements with the type
 */
public class ArrayColumn extends Column {

    private final Column element;

    public ArrayColumn(int id, String name, boolean required, Column element) {
        super(id, name, String.format("array<%s>", element.type()), required);
        this.element = element;
    }

    public Column getElement() {
        return element;
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
        ArrayColumn that = (ArrayColumn) o;
        return Objects.equals(element, that.element);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), element);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ArrayColumn{");
        sb.append("element=").append(element);
        sb.append('}');
        return sb.toString();
    }
}
