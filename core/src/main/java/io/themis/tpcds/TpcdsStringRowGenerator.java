package io.themis.tpcds;

import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.themis.Table;
import io.themis.TableDataGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.UUID;

public class TpcdsStringRowGenerator implements TableDataGenerator<String[]> {

    private static final Logger LOG = LoggerFactory.getLogger(TpcdsStringRowGenerator.class);

    private static final String TPCDS_DSDGEN_RESOURCE_PATH = "tpcds/" + getOsPath() + "/dsdgen";
    private static final String TPCDS_IDX_RESOURCE_PATH = "tpcds/" + getOsPath() + "/tpcds.idx";
    private static final int TEMP_DIR_CREATE_ATTEMPTS_MAX = 10;
    private static final String TEMP_DIR_ROOT = System.getProperty("java.io.tmpdir");
    private static final String TEMP_DIR_PREFIX = "abs-tpcds-dsdgen-";

    private final Table table;
    private final int scaleFactor;
    private final int parallelism;
    private final int id;

    public TpcdsStringRowGenerator(Table table, int scaleFactor, int parallelism, int id) {
        this.table = table;
        this.scaleFactor = scaleFactor;
        this.parallelism = parallelism;
        this.id = id;
    }

    @Override
    public Table table() {
        return table;
    }

    private static class TpcdsStringRowIterator implements Iterator<String[]>, Serializable {

        private final Table table;
        private final int scaleFactor;
        private final int parallelism;
        private final int id;

        private BufferedReader reader;
        private String[] row;
        private boolean available;

        public TpcdsStringRowIterator(Table table, int scaleFactor, int parallelism, int id) {
            this.table = table;
            this.scaleFactor = scaleFactor;
            this.parallelism = parallelism;
            this.id = id;
        }

        @Override
        public boolean hasNext() {
            if (available) {
                return row != null;
            }

            return fetchNextRow();
        }

        @Override
        public String[] next() {
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
            if (reader == null) {
                reader = initializeDsdgen(getClass().getClassLoader(),
                        table.name(), scaleFactor, parallelism, id);
            }

            String line;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read new line in dsdgen", e);
            }
            available = true;

            if (line == null) {
                return false;
            }

            row = line.split("\\|", table.columns().size());
            for (int i = 0; i < row.length - 1; i++) {
                if (row[i].isEmpty()) {
                    row[i] = null;
                }
            }

            // last value must be together with the last | delimiter, needs to be removed
            String lastValue = row[row.length - 1];
            row[row.length - 1] = lastValue.substring(0, lastValue.length() - 1);
            return true;
        }
    }

    @Override
    public Iterator<String[]> iterator() {
        return new TpcdsStringRowIterator(table, scaleFactor, parallelism, id);
    }

    private static BufferedReader initializeDsdgen(
            ClassLoader classLoader,
            String tableName,
            int scaleFactor,
            int parallelism,
            int id) {
        File dir = createTempDirectory();
        copyResourceToDir(classLoader, TPCDS_DSDGEN_RESOURCE_PATH, dir);
        copyResourceToDir(classLoader, TPCDS_IDX_RESOURCE_PATH, dir);

        File dsdgen = new File(dir, TPCDS_DSDGEN_RESOURCE_PATH);
        if (!dsdgen.setExecutable(true)) {
            throw new RuntimeException(
                    "Can't set a executable flag on: " + dsdgen.getAbsolutePath());
        }

        // first cd to the parent directory and then run dsdgen
        // because it must be executed in the same relative path as tpcds.idx
        // otherwise tpcds.idx not found error will be thrown
        // -filter Y prints output to STDOUT
        String command = String.format("cd %s; ./%s -table %s -filter Y -scale %d",
                dsdgen.getParentFile().getAbsolutePath(),
                dsdgen.getName(),
                tableName,
                scaleFactor);
        if (parallelism > 1) {
            command += String.format(" -parallel %d -child %d", parallelism, id);
        }

        LOG.info("Executing command: " + command);
        ProcessBuilder pb = new ProcessBuilder();
        pb.command("bash", "-c", command);
        try {
            Process process = pb.start();
            // TODO: default buffer size can be estimated for each table
            //  to minimize memory consumption
            return new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot execute command: " + command, e);
        }
    }

    private static File createTempDirectory() {
        int attempts = 0;
        File dir = null;
        while (dir == null) {
            attempts += 1;
            if (attempts > TEMP_DIR_CREATE_ATTEMPTS_MAX) {
                throw new IllegalStateException(String.format(
                        "Failed to create a temp directory for TPC-DS dsdgen under %s after %d attempts",
                        TEMP_DIR_ROOT, TEMP_DIR_CREATE_ATTEMPTS_MAX));
            }
            try {
                dir = new File(TEMP_DIR_ROOT, TEMP_DIR_PREFIX + UUID.randomUUID());
                if (dir.exists() || !dir.mkdirs()) {
                    dir = null;
                }
            } catch (SecurityException e) {
                dir = null;
            }
        }

        File tempDir;
        try {
            tempDir = dir.getCanonicalFile();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Unexpected error when accessing directory " +
                            dir.getAbsolutePath(), e);
        }

        // remove folder after JVM exits
        tempDir.deleteOnExit();
        LOG.info("Created temporary directory: " + tempDir.getAbsolutePath());
        return tempDir;
    }

    private static void copyResourceToDir(ClassLoader classLoader, String resourceName, File dir) {
        File fileToCreate = new File(dir, resourceName);
        // make any missing directories
        fileToCreate.getParentFile().mkdirs();

        try (InputStream in = classLoader.getResourceAsStream(resourceName);
             OutputStream out = new FileOutputStream(fileToCreate)) {
            if (in == null) {
                throw new IllegalStateException("Cannot find resource in Java class path: " + resourceName);
            }
            ByteStreams.copy(in, out);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format(
                    "Cannot copy resource %s content to %s",
                    resourceName, dir.getAbsolutePath()), e);
        }
        LOG.info("Copied resource {} to path {}", resourceName, fileToCreate.getAbsolutePath());
    }

    private static String getOsPath() {
        String osName = System.getProperty("os.name");
        if (osName != null && osName.toLowerCase(Locale.ENGLISH).contains("mac")) {
            return "mac";
        }
        return "linux";
    }
}
