package io.themis.partition;

public class DayTransform extends PartitionTransform {

    public DayTransform(int columnId) {
        super(columnId, "day");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DayTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
