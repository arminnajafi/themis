package io.themis.partition;

public class MonthTransform extends PartitionTransform {

    public MonthTransform(int columnId) {
        super(columnId, "month");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MonthTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
