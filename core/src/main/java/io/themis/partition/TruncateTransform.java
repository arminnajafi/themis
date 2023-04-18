package io.themis.partition;

import io.themis.column.Column;

import java.util.Objects;

public class TruncateTransform extends PartitionTransform {

    private final int width;

    public TruncateTransform(int columnId, int width) {
        super(columnId, String.format("truncate(%d)", width));
        this.width = width;
    }

    public int getWidth() {
        return width;
    }

    @Override
    public String partitionTransformName(Column column) {
        return String.format("truncate(%d,'%s')", width, column.name());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TruncateTransform that = (TruncateTransform) o;
        return width == that.width;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), width);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TruncateTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append(", width=").append(width);
        sb.append('}');
        return sb.toString();
    }
}
