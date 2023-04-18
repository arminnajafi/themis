package io.themis.partition;

import io.themis.column.Column;

import java.util.Objects;

public class BucketTransform extends PartitionTransform {

    private final int bucket;

    public BucketTransform(int columnId, int bucket) {
        super(columnId, String.format("bucket(%d)", bucket));
        this.bucket = bucket;
    }

    public int getBucket() {
        return bucket;
    }

    @Override
    public String partitionTransformName(Column column) {
        return String.format("bucket(%s,%d)", column.name(), bucket);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        BucketTransform that = (BucketTransform) o;
        return bucket == that.bucket;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucket);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BucketTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append(", bucket=").append(bucket);
        sb.append('}');
        return sb.toString();
    }
}
