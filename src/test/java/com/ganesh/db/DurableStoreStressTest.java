package com.ganesh.db;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A stress test to verify the stability and data integrity of the DurableStore under heavy concurrent load.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // Allows non-static @BeforeAll and @AfterAll
class DurableStoreStressTest {

    private KeyValueStore store;
    private final Config config = new Config.Builder()
            .withDataDirectory("stress_test_data/")
            .build();

    // --- Test Parameters ---
    private static final int NUM_THREADS = 8;
    private static final int TOTAL_OPERATIONS = 50_000;
    private static final int KEY_RANGE = 1_000; // Operations will be on keys "key-0" to "key-999"

    @BeforeAll
    void setUp() throws IOException {
        System.out.println("Setting up stress test...");
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
        System.out.println("Setup complete. Store started.");
    }

    @AfterAll
    void tearDown() {
        System.out.println("Stress test finished. Shutting down store.");
        if (store != null) {
            System.out.println("Final Metrics Report:");
            System.out.println(store.getMetrics().getSummary());
            store.stop();
        }
    }

    @Test
    void runConcurrentStressTest() throws InterruptedException {
        System.out.printf("Starting stress test with %d threads, %d operations over %d unique keys.\n",
                NUM_THREADS, TOTAL_OPERATIONS, KEY_RANGE);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        // This map will act as our "ground truth" to verify data integrity at the end.
        final ConcurrentMap<String, String> groundTruth = new ConcurrentHashMap<>();

        List<Callable<Void>> tasks = new ArrayList<>();
        final Random random = new Random();

        for (int i = 0; i < TOTAL_OPERATIONS; i++) {
            tasks.add(() -> {
                String key = "key-" + random.nextInt(KEY_RANGE);
                double operation = random.nextDouble();

                // Workload: 50% reads, 40% writes, 10% deletes
                if (operation < 0.5) { // GET
                    store.get(key);
                } else if (operation < 0.9) { // PUT
                    String value = "value-" + Thread.currentThread().getId() + "-" + System.nanoTime();
                    store.put(key, value);
                    groundTruth.put(key, value); // Update ground truth
                } else { // DELETE
                    store.delete(key);
                    groundTruth.remove(key); // Update ground truth
                }
                return null;
            });
        }

        // Run all tasks and wait for them to complete
        executor.invokeAll(tasks);
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        System.out.println("All operations completed. Verifying data integrity...");

        // --- Verification Phase ---
        // Wait a moment for any final compactions or flushes to settle.
        Thread.sleep(2000);

        for (int i = 0; i < KEY_RANGE; i++) {
            String key = "key-" + i;
            String expectedValue = groundTruth.get(key);
            String actualValue = store.get(key).orElse(null);
            assertEquals(expectedValue, actualValue, "Data mismatch for key: " + key);
        }
        System.out.println("Data integrity verification successful!");
    }
}