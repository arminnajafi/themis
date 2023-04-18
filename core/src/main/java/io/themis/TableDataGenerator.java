package io.themis;

import java.io.Serializable;

/**
 * Generate rows in a table
 *
 * @param <T> object class that represents a row
 */
public interface TableDataGenerator<T> extends Iterable<T>, Serializable {

    Table table();
}
