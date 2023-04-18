package io.themis;

import io.themis.column.Column;
import io.themis.column.StructColumn;
import io.themis.partition.PartitionTransform;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Table implements Serializable {

    private final String name;
    private final List<Column> columns;
    private final List<PartitionTransform> partitionKeys;
    private final List<Integer> primaryKeys;
    private final Map<Integer, Column> columnIndex;

    public Table(
            String name,
            List<Column> columns,
            List<PartitionTransform> partitionKeys,
            List<Integer> primaryKeys) {
        this.name = name;
        this.columns = columns;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        columnIndex = columns.stream()
                .collect(Collectors.toMap(Column::id, Function.identity()));
    }

    public String name() {
        return name;
    }

    public List<Column> columns() {
        return columns;
    }

    public StructColumn schema() {
        return new StructColumn(1, "schema", false, columns);
    }

    public List<PartitionTransform> partitionKeys() {
        return partitionKeys;
    }

    public Map<PartitionTransform, Column> partitionKeyColumns() {
        return partitionKeys.stream()
                .collect(
                        Collectors.toMap(Function.identity(),
                        key -> columnIndex.get(key.columnId())));
    }

    public List<Integer> primaryKeys() {
        return primaryKeys;
    }

    public List<Column> primaryKeyColumns() {
        return primaryKeys.stream()
                .map(columnIndex::get)
                .collect(Collectors.toList());
    }

    public String ddlSchemaPart() {
        StringBuilder sb = new StringBuilder();
        for (Column column : columns) {
            sb.append(column.name());
            sb.append(' ');
            sb.append(column.type());
            sb.append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public Table withPartitionKeys(List<PartitionTransform> newKeys) {
        return new Table(name, columns, newKeys, primaryKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return Objects.equals(name, table.name) &&
                Objects.equals(columns, table.columns) &&
                Objects.equals(partitionKeys, table.partitionKeys) &&
                Objects.equals(primaryKeys, table.primaryKeys) &&
                Objects.equals(columnIndex, table.columnIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columns, partitionKeys, primaryKeys, columnIndex);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Table{");
        sb.append("name='").append(name).append('\'');
        sb.append(", columns=").append(columns);
        sb.append(", partitionKeys=").append(partitionKeys);
        sb.append(", primaryKeys=").append(primaryKeys);
        sb.append(", columnIndex=").append(columnIndex);
        sb.append('}');
        return sb.toString();
    }
}
