package io.themis.datagen;

import io.themis.Table;
import io.themis.TableDataGenerator;
import io.themis.column.Column;
import io.themis.column.ColumnVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class RandomRowGenerator implements TableDataGenerator<Object[]> {

    private static final Logger LOG = LoggerFactory.getLogger(RandomRowGenerator.class);

    private final Table table;
    private final long rowCount;
    private final ColumnVisitor<Object> gen;

    public RandomRowGenerator(Table table, ColumnVisitor<Object> gen, long rowCount) {
        this.table = table;
        this.rowCount = rowCount;
        this.gen = gen;
    }

    @Override
    public Table table() {
        return table;
    }

    private static class RandomRowIterator implements Iterator<Object[]>, Serializable {

        private final List<Column> columns;
        private final int columnCount;
        private final long rowCount;
        private final ColumnVisitor<Object> gen;

        private Object[] row;
        private boolean available;
        private long current = 0;

        public RandomRowIterator(Table table, long rowCount, ColumnVisitor<Object> gen) {
            this.columns = table.columns();
            this.columnCount = table.columns().size();
            this.rowCount = rowCount;
            this.gen = gen;
        }

        @Override
        public boolean hasNext() {
            if (available) {
                return row != null;
            }

            return fetchNextRow();
        }

        @Override
        public Object[] next() {
            if (!available) {
                fetchNextRow();
            }

            if (!available) {
                throw new NoSuchElementException();
            }

            available = false;
            return row;
        }

        private boolean fetchNextRow() {
            available = true;
            current++;

            if (rowCount < current) {
                row = null;
                return false;
            }

            row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = columns.get(i).accept(gen);
            }

            return true;
        }
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new RandomRowIterator(table, rowCount, gen);
    }
}
