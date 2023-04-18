package io.themis.spark.datagen;

import io.themis.Table;
import io.themis.datagen.SimpleTables;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkRandomRowGeneratorTest {

    private SparkSession spark;

    @BeforeEach
    public void before() {
        spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();
    }

    @AfterEach
    public void after() {
        spark.sql("DROP DATABASE IF EXISTS themis");
    }

    @Test
    public void testCreateDFDefault() {
        SparkRandomRowGeneratorConfig args = new SparkRandomRowGeneratorConfig(new String[]{});
        SparkRandomRowGenerator gen = new SparkRandomRowGenerator(spark, args);
        for (Table table : SimpleTables.TABLES.values()) {
            Dataset<Row> rows = gen.createDF(table);
            Assertions.assertThat(rows.count()).isEqualTo(100);
        }
    }

    @Test
    public void testCreateDFWithParallelism() {
        SparkRandomRowGeneratorConfig args = new SparkRandomRowGeneratorConfig(
                new String[]{"--parallelism", "10"});
        SparkRandomRowGenerator gen = new SparkRandomRowGenerator(spark, args);
        for (Table table : SimpleTables.TABLES.values()) {
            Dataset<Row> rows = gen.createDF(table);
            Assertions.assertThat(rows.count()).isEqualTo(100);
        }
    }
}