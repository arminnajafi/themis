package io.themis.partition;

public class YearTransform extends PartitionTransform {

    public YearTransform(int columnId) {
        super(columnId, "year");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("YearTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
