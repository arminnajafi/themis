package io.themis.partition;

public class HourTransform extends PartitionTransform {

    public HourTransform(int columnId) {
        super(columnId, "hour");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HourTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
