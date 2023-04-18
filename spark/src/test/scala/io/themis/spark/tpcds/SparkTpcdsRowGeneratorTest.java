package io.themis.spark.tpcds;

import com.google.common.collect.ImmutableList;
import io.themis.tpcds.TpcdsTables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;
import io.themis.Table;

public class SparkTpcdsRowGeneratorTest {

    private SparkSession spark;

    @BeforeEach
    public void before() {
        spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();
    }

    @AfterEach
    public void after() {
        spark.sql("DROP DATABASE IF EXISTS test");
    }

    @Test
    public void testCreateDFDefault() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(new String[]{});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        for (Table table : ImmutableList.of(
                TpcdsTables.CATALOG_SALES,
                TpcdsTables.INVENTORY,
                TpcdsTables.CALL_CENTER,
                TpcdsTables.CUSTOMER_DEMOGRAPHICS,
                TpcdsTables.WEB_SITE)) {
            Dataset<Row> rows = gen.createDF(table);
            Assertions.assertThat(rows.count()).isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(table));
        }
    }

    @Test
    public void testCreateDFWithParallelism() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--parallelism", "10"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        for (Table table : ImmutableList.of(
                TpcdsTables.CATALOG_SALES,
                TpcdsTables.INVENTORY,
                TpcdsTables.CALL_CENTER,
                TpcdsTables.CUSTOMER_DEMOGRAPHICS,
                TpcdsTables.WEB_SITE)) {
            Dataset<Row> rows = gen.createDF(table);
            Assertions.assertThat(rows.count()).isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(table));
        }
    }

    @Test
    public void testCreateDFRepartitionTables() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--repartition-tables"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        for (Table table : ImmutableList.of(
                TpcdsTables.CATALOG_SALES,
                TpcdsTables.INVENTORY,
                TpcdsTables.CALL_CENTER,
                TpcdsTables.CUSTOMER_DEMOGRAPHICS,
                TpcdsTables.WEB_SITE)) {
            Dataset<Row> rows = gen.createDF(table);
            Assertions.assertThat(rows.count()).isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(table));
        }
    }

    @Test
    public void testCreateDFUseStringForChar() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--use-string-for-char"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        Dataset<Row> rows = gen.createDF(TpcdsTables.CALL_CENTER);
        Assertions.assertThat(rows.count()).isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(TpcdsTables.CALL_CENTER));
        Option<StructField> field = rows.schema().find(f -> f.name().equals("cc_call_center_id"));
        Assertions.assertThat(field.nonEmpty()).isTrue();
        Assertions.assertThat(field.get().dataType()).isInstanceOf(StringType.class);
    }

    @Test
    public void testCreateDFUseDoubleForDecimal() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--use-double-for-decimal"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        Dataset<Row> rows = gen.createDF(TpcdsTables.CALL_CENTER);
        Assertions.assertThat(rows.count()).isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(TpcdsTables.CALL_CENTER));
        Option<StructField> field = rows.schema().find(f -> f.name().equals("cc_gmt_offset"));
        Assertions.assertThat(field.nonEmpty()).isTrue();
        Assertions.assertThat(field.get().dataType()).isInstanceOf(DoubleType.class);
    }

    @Test
    public void testCreateDFFilterOutNullPartitionValues() {
        // TODO: need to figure out if anything would change in this at what scale factor
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--filter-out-null-partition-values"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        for (Table table : ImmutableList.of(TpcdsTables.CATALOG_RETURNS)) {
            Dataset<Row> rows = gen.createDF(table);
            Assertions.assertThat(rows.count()).isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(table));
        }
    }

    @Test
    public void testGetTablesDefault() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        Assertions.assertThat(JavaConverters.asJavaCollection(gen.tables()))
                .containsExactlyInAnyOrder(TpcdsTables.TABLES.values().toArray(new Table[0]));
    }

    @Test
    public void testGetTablesFilter() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--filter", "catalog_returns,inventory"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        Assertions.assertThat(JavaConverters.asJavaCollection(gen.tables()))
                .containsExactlyInAnyOrder(TpcdsTables.CATALOG_RETURNS, TpcdsTables.INVENTORY);
    }

    @Test
    public void testGetTablesUnpartitioned() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--variant", "unpartitioned"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
       Assertions.assertThat(JavaConverters.asJavaCollection(gen.tables()))
                .containsExactlyInAnyOrder(TpcdsTables.UNPARTITIONED_TABLES.values().toArray(new Table[0]));
    }

    @Test
    public void testGetTablesInvalid() {
        SparkTpcdsRowGeneratorConfig args = new SparkTpcdsRowGeneratorConfig(
                new String[]{"--variant", "invalid"});
        SparkTpcdsRowGenerator gen = new SparkTpcdsRowGenerator(spark, args);
        Assertions.assertThatThrownBy(gen::tables)
                .isInstanceOf(IllegalArgumentException.class);
    }
}