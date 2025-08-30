package com.ganesh.db;

import java.util.concurrent.atomic.LongAdder;

/**
 * A thread-safe class for tracking key performance indicators of the database.
 * Uses LongAdder for high-performance counting under concurrent access.
 */
public class StoreMetrics {
    public final LongAdder puts = new LongAdder();
    public final LongAdder gets = new LongAdder();
    public final LongAdder deletes = new LongAdder();
    public final LongAdder walWrites = new LongAdder();
    public final LongAdder memtableFlushes = new LongAdder();
    public final LongAdder compactions = new LongAdder();
    public final LongAdder bloomFilterChecks = new LongAdder();
    public final LongAdder bloomFilterHits = new LongAdder(); // A "hit" is when the filter saves us a disk read

    // For latency tracking
    private final LongAdder totalGetLatencyNanos = new LongAdder();
    private final LongAdder getSamples = new LongAdder();

    /**
     * Records the duration of a get operation.
     * @param nanos The duration of the operation in nanoseconds.
     */
    public void recordGetLatency(long nanos) {
        totalGetLatencyNanos.add(nanos);
        getSamples.increment();
    }

    /**
     * Calculates the average latency for get operations in milliseconds.
     * @return The average latency in ms, or 0 if no samples have been recorded.
     */
    public double getAverageGetLatencyMs() {
        long sampleCount = getSamples.sum();
        if (sampleCount == 0) return 0.0;
        return (totalGetLatencyNanos.sum() / (double) sampleCount) / 1_000_000.0;
    }

    /**
     * Generates a human-readable summary of all collected metrics.
     * @return A string containing the metrics summary.
     */
    public String getSummary() {
        long checks = bloomFilterChecks.sum();
        long hits = bloomFilterHits.sum();
        double hitRate = checks == 0 ? 0.0 : (hits * 100.0) / checks;

        return String.format(
            "--- Store Metrics ---\n" +
            "Operations -> Puts: %,d | Gets: %,d | Deletes: %,d\n" +
            "Latency    -> Avg Get: %.3f ms\n" +
            "Internals  -> WAL Writes: %,d | Memtable Flushes: %,d | Compactions: %,d\n" +
            "Bloom Filter -> Hits: %,d / %,d (%.2f%% hit rate)\n" +
            "---------------------",
            puts.sum(), gets.sum(), deletes.sum(),
            getAverageGetLatencyMs(),
            walWrites.sum(), memtableFlushes.sum(), compactions.sum(),
            hits, checks, hitRate
        );
    }
}