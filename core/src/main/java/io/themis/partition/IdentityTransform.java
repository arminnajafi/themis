package io.themis.partition;

import io.themis.column.Column;

public class IdentityTransform extends PartitionTransform {

    public IdentityTransform(int columnId) {
        super(columnId, "identity");
    }

    @Override
    public String partitionTransformName(Column column) {
        return column.name();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IdentityTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
