package io.themis;

import com.google.common.collect.ImmutableMap;
import io.themis.datagen.SimpleTables;
import io.themis.tpcds.TpcdsTables;

import java.util.Map;

public class Datasets {

    private static final Map<String, Map<String, Table>> DATASETS = ImmutableMap.<String, Map<String, Table>>builder()
            .put("tpcds", TpcdsTables.TABLES)
            .put("tpcds_unpartitioned", TpcdsTables.UNPARTITIONED_TABLES)
            .put("simple", SimpleTables.TABLES)
            .build();

    public static Map<String, Map<String, Table>> all() {
        return DATASETS;
    }
}
