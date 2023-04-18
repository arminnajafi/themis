package io.themis.partition;

import io.themis.column.Column;

/**
 * Represents an external partition value,
 * where the column value is not present in the underlying data files,
 * but only registered in a Hive metastore as metadata information.
 */
public class ExternalTransform extends PartitionTransform {

    public ExternalTransform(int columnId) {
        super(columnId, "external");
    }

    @Override
    public String partitionTransformName(Column column) {
        return column.name();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExternalTransform{");
        sb.append("columnId=").append(columnId());
        sb.append(", signature='").append(signature()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
