package com.ganesh.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DurableStoreBenchmarkTest {

    private DurableStore store;
    private final Config config = new Config.Builder() 
            .withDataDirectory("test_data/")
            .withMemtableThresholdBytes(1024)
            .build();
    private static final int NUM_KEYS = 20_000;
    private static final int NUM_READS = 5_000;

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
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

        System.out.println("Setting up benchmark: Writing " + NUM_KEYS + " keys...");
        for (int i = 0; i < NUM_KEYS; i++) {
            String key = String.format("key-%05d", i);
            String value = "value-for-key-" + i + "-some-extra-padding-to-make-values-larger";
            store.put(key, value);
        }

        // Wait for all flushes and compactions to complete to get a stable state
        System.out.println("Waiting for storage engine to settle...");
        Thread.sleep(2000);
        System.out.println("Setup complete.");
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.stop();
        }
    }

    @Test
    void benchmarkRandomReads() {
        List<String> keysToRead = new ArrayList<>();
        for (int i = 0; i < NUM_KEYS; i++) {
            keysToRead.add(String.format("key-%05d", i));
        }
        Collections.shuffle(keysToRead);

        System.out.println("Starting benchmark: Performing " + NUM_READS + " random reads...");

        long startTime = System.nanoTime();

        for (int i = 0; i < NUM_READS; i++) {
            store.get(keysToRead.get(i));
        }

        long endTime = System.nanoTime();
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

        System.out.println("----------------------------------------");
        System.out.println("Benchmark Results");
        System.out.println("----------------------------------------");
        System.out.printf("Total reads performed: %,d\n", NUM_READS);
        System.out.printf("Total time taken: %,d ms\n", durationMillis);
        if (durationMillis > 0) {
            double readsPerSecond = (NUM_READS / (double) durationMillis) * 1000.0;
            System.out.printf("Reads per second: %,.2f\n", readsPerSecond);
        }
        System.out.println("----------------------------------------");
    }
}