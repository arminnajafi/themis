package io.themis.spark.iceberg;

import io.themis.tpcds.TpcdsTables;
import org.apache.iceberg.TableProperties;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class IcebergDatagenTest {

    private SparkSession spark;

    @AfterEach
    public void after() {
        spark.sql("DROP DATABASE IF EXISTS test CASCADE");
    }

    @Disabled("Can run in Intellij, but will cause OOM when running in CLI")
    @Test
    public void testDatagenPartitionedCorrectness(@TempDir Path tempDir) {
        spark = IcebergDatagen.doMain(new String[]{
                "--database-name", "test",
                "--database-location", tempDir.toAbsolutePath().toString(),
                "--partition-tables",
                "--repartition-tables",
                "--filter", "catalog_returns",
                "--parallelism", "10",
                "--replace-tables",
                "--local"
        });

        Assertions.assertThat((long) spark.sql("SELECT count(*) FROM test.catalog_returns").collectAsList().get(0).get(0))
                .isEqualTo((long) TpcdsTables.SF1_ROW_COUNTS.get(TpcdsTables.CATALOG_RETURNS));

        Assertions.assertThat((long) spark.sql("SELECT count(*) FROM test.catalog_returns.partitions").collectAsList().get(0).get(0))
                .isEqualTo((long) spark.sql("SELECT count(distinct(cr_returned_date_sk)) FROM test.catalog_returns").collectAsList().get(0).get(0));
    }

    @Test
    public void testDatagenUnpartitionedCorrectness(@TempDir Path tempDir) {
        spark = IcebergDatagen.doMain(new String[]{
                "--database-name", "test",
                "--database-location", tempDir.toAbsolutePath().toString(),
                "--filter", "call_center",
                "--use-string-for-char",
                "--replace-tables",
                "--local"
        });

        List<Object[]> rows = spark.sql("SELECT * FROM test.call_center").collectAsList()
                .stream()
                .map(row -> JavaConverters.asJavaCollection(row.toSeq().toList()))
                .map(lst -> lst.stream()
                        .map(v -> v == null ? null : String.valueOf(v))
                        .collect(Collectors.toList())
                        .toArray(new Object[0]))
                // make numbers like "5.00" as "5"
                .peek(arr -> arr[arr.length - 2] = ((String) arr[arr.length - 2])
                        .substring(0, ((String) arr[arr.length - 2]).length() - 3))
                .collect(Collectors.toList());
        Assertions.assertThat(rows)
                .containsExactlyInAnyOrder(TpcdsTables.CALL_CENTER_DATA.toArray(new Object[0][]));
    }

    @Test
    public void testDatagenParameters(@TempDir Path tempDir) {
        spark = IcebergDatagen.doMain(new String[]{
                "--database-name", "test",
                "--database-location", tempDir.toAbsolutePath().toString(),
                "--filter", "call_center",
                "--object-storage-mode",
                "--file-format", "orc",
                "--compression-codec", "snappy",
                "--replace-tables",
                "--local"
        });

        Map<String, String> properties = spark.sql("SHOW TBLPROPERTIES test.call_center").collectAsList()
                .stream()
                .collect(Collectors.toMap(row -> (String) row.get(0), row -> (String) row.get(1)));

        Assertions.assertThat(properties)
                .containsEntry(TableProperties.OBJECT_STORE_ENABLED, "true")
                .containsEntry(TableProperties.FORMAT_VERSION, "2")
                .containsEntry(TableProperties.DEFAULT_FILE_FORMAT, "orc")
                .containsEntry(TableProperties.ORC_COMPRESSION, "snappy");
    }

    @Test
    public void testDatagenRandom(@TempDir Path tempDir) {
        spark = IcebergDatagen.doMain(new String[]{
                "--gen", "random",
                "--database-name", "test",
                "--database-location", tempDir.toAbsolutePath().toString(),
                "--filter", "users",
                "--parallelism", "10",
                "--row-count", "1000",
                "--replace-tables",
                "--local"
        });

        List<Object[]> rows = spark.sql("SELECT * FROM test.users").collectAsList()
                .stream()
                .map(row -> JavaConverters.asJavaCollection(row.toSeq().toList()))
                .map(lst -> lst.stream()
                        .map(v -> v == null ? null : String.valueOf(v))
                        .collect(Collectors.toList())
                        .toArray(new Object[0]))
                .collect(Collectors.toList());
        Assertions.assertThat(rows).hasSize(1000);
    }
}