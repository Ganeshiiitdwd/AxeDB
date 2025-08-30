package com.ganesh.db;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A dedicated benchmark to measure the raw data throughput (MB/sec) of the DurableStore.
 * <p>
 * This test uses a large payload for each write operation to saturate the data pipeline
 * and find the bottleneck in terms of data volume rather than operation count.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataThroughputBenchmarkTest {

    private KeyValueStore store;
    private final Config config = new Config.Builder()
            .withDataDirectory("data_benchmark_data/")
            .build(); // Uses the performance-tuned config

    // --- Benchmark Parameters ---
    private static final int TOTAL_WRITE_OPERATIONS = 1000; 
    private static final int VALUE_SIZE_BYTES = 5120; // 5 KB value per operation
    private static final String LARGE_VALUE = generateLargeString(VALUE_SIZE_BYTES);

    /**
     * Helper method to create a large string of a specific size.
     * @param size The desired size of the string in bytes (approximate).
     * @return A large string for use as a value.
     */
    private static String generateLargeString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    @BeforeAll
    void setUp() throws IOException {
        System.out.println("\n--- Setting up Data Throughput Benchmark ---");
        File dataDir = new File(config.getDataDirectory());
        if (dataDir.exists()) {
            Files.walk(dataDir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        dataDir.mkdirs();
        store = new DurableStore(config);
        store.start();
        System.out.println("Setup complete.");
    }

    @AfterAll
    void tearDown() {
        System.out.println("--- Data Throughput Benchmark Finished ---");
        if (store != null) {
            store.stop();
        }
    }

    @Test
    void runDataThroughputBenchmark() {
        System.out.printf("Executing benchmark: Inserting %,d keys with a %,d-byte value each...%n",
                TOTAL_WRITE_OPERATIONS, VALUE_SIZE_BYTES);

        long startTime = System.nanoTime();
        long totalBytesWritten = 0;

        for (int i = 0; i < TOTAL_WRITE_OPERATIONS; i++) {
            String key = Integer.toString(i);
            store.put(key, LARGE_VALUE);
            totalBytesWritten += (key.length() * 2L) + (LARGE_VALUE.length() * 2L);
        }

        long endTime = System.nanoTime();
        System.out.println("Benchmark execution complete.");

        // --- Calculate and Report Results ---
        long durationNanos = endTime - startTime;
        double durationSeconds = durationNanos / 1_000_000_000.0;
        double throughputOps = TOTAL_WRITE_OPERATIONS / durationSeconds;
        double totalDataMB = totalBytesWritten / (1024.0 * 1024.0);
        double throughputMBps = totalDataMB / durationSeconds;

        System.out.println("\n======================================");
        System.out.println("   Data Throughput Benchmark Results    ");
        System.out.println("======================================");
        System.out.printf("Total Write Operations: %,d%n", TOTAL_WRITE_OPERATIONS);
        System.out.printf("Total Data Written:     %.2f MB%n", totalDataMB);
        System.out.printf("Total Execution Time:   %.2f seconds%n", durationSeconds);
        System.out.println("--------------------------------------");
        System.out.printf("--> Ops Throughput:     %,.0f ops/sec%n", throughputOps);
        System.out.printf("--> Data Throughput:    %.2f MB/sec%n", throughputMBps);
        System.out.println("======================================\n");

        assertTrue(throughputMBps > 50, "Expected high data throughput.");
    }
}
