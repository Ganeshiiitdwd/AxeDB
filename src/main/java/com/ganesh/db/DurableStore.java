package com.ganesh.db;

import com.ganesh.db.sstable.SSTableManager;
import com.ganesh.db.sstable.SSTableReader;
import com.ganesh.db.sstable.SSTableWriter;
import com.ganesh.db.wal.LogEntry;
import com.ganesh.db.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A persistent, durable key-value store implementation based on a Log-Structured Merge-Tree (LSM-Tree) architecture.
 *
 * <p>This class orchestrates the primary components of the database:
 * <ul>
 * <li><b>Memtable:</b> An in-memory, sorted map ({@link ConcurrentSkipListMap}) for fast writes.</li>
 * <li><b>Write-Ahead Log (WAL):</b> Ensures durability by logging all operations to disk before applying them to the memtable.</li>
 * <li><b>SSTables (Sorted String Tables):</b> Immutable files on disk that store flushed memtable data.</li>
 * </ul>
 * All write operations (put/delete) are first written to the WAL and then to the memtable. When the memtable
 * reaches a configured size threshold, it is asynchronously flushed to a new SSTable on disk.
 *
 * <p>Read operations first check the memtable and then query the SSTables from newest to oldest.
 *
 * @see KeyValueStore
 * @see WriteAheadLog
 * @see SSTableManager
 */

@SuppressWarnings("unused")
public class DurableStore implements KeyValueStore {
    /**
     * A single-threaded executor to serialize memtable flush operations. This ensures that
     * SSTables are written to disk sequentially, avoiding race conditions and maintaining order.
     */
    private final ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
    private static final Logger logger = LoggerFactory.getLogger(DurableStore.class);

    /**
     * The in-memory write buffer. It is declared {@code volatile} to ensure visibility of the reference
     * across threads when it is swapped during a flush operation.
     */
    private volatile ConcurrentSkipListMap<String, String> memtable;

    /**
     * The approximate size of the current memtable in bytes. Used to trigger a flush.
     */
    private long memtableSizeInBytes;
    private Config config;
    private WriteAheadLog wal;
    final SSTableWriter sstableWriter;
    final SSTableManager sstableManager;
    final StoreMetrics metrics; 

    /**
     * Constructs a new DurableStore with the specified configuration.
     *
     * @param config The configuration object containing parameters like data directory and memtable thresholds.
     */
    public DurableStore(Config config) {
        this.config = config;
        this.metrics = new StoreMetrics(); 
        this.memtable = new ConcurrentSkipListMap<>();
        this.sstableWriter = new SSTableWriter(config);
        this.sstableManager = new SSTableManager(config,metrics);
        this.memtableSizeInBytes = 0;
    }

    /**
     * Starts the key-value store. This process involves creating the data directory if it
     * doesn't exist, loading all existing SSTables from disk, and recovering the in-memory
     * state by replaying the Write-Ahead Log.
     *
     * @throws RuntimeException if initialization fails due to an I/O error.
     */
    @Override
    public void start() {
        logger.info("Durable Store starting...");
        try {
            File dataDir = new File(config.getDataDirectory());
            if (!dataDir.exists() && !dataDir.mkdirs()) {
                throw new IOException("Failed to create data directory: " + dataDir);
            }

            sstableManager.loadSSTables();
            this.wal = new WriteAheadLog(config.getDataDirectory());
            recoverStateFromWal();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize store", e);
        }
        logger.info("Durable Store started successfully.");
    }

    /**
     * Recovers the state of the memtable by replaying all entries from the Write-Ahead Log.
     * This method is called during startup to ensure no data is lost from a previous crash.
     *
     * @throws IOException if there is an error reading from the WAL files.
     */
    private void recoverStateFromWal() throws IOException {
        WriteAheadLog.replayAll(this.memtable);
        this.memtableSizeInBytes = 0;
        for (var entry : memtable.entrySet()) {
            memtableSizeInBytes += entry.getKey().length() + (entry.getValue() != null ? entry.getValue().length() : 0);
        }
    }

    /**
     * Stops the key-value store, ensuring all resources are closed gracefully.
     * This includes shutting down the flush executor and closing the WAL and SSTable managers.
     */
    @Override
    public void stop() {
        try {
            flushExecutor.shutdown();
            if (!flushExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("Flush executor did not terminate in time.");
            }
            if (wal != null) wal.close();
            sstableManager.close();
        } catch (IOException | InterruptedException e) {
            logger.error("Failed to close resources cleanly.", e);
        }
    }

    /**
     * Retrieves the metrics collector for this store instance.
     *
     * @return The {@link StoreMetrics} object used for tracking performance.
     */
    @Override
    public StoreMetrics getMetrics() {
        return this.metrics;
    }

    /**
     * Inserts or updates a key-value pair. The operation is first written to the WAL for durability,
     * then added to the memtable. If the memtable exceeds its size threshold, a flush to an SSTable
     * is triggered.
     *
     * @param key The key to insert or update.
     * @param value The value associated with the key.
     * @throws RuntimeException if the write to the WAL fails.
     */
    @Override
    public void put(String key, String value) {
         metrics.puts.increment();
        try {
            LogEntry entry = new LogEntry(key, value);
            wal.writeEntry(entry);
            metrics.walWrites.increment();
            memtable.put(key, value);
            memtableSizeInBytes += key.length() + value.length();

            if (memtableSizeInBytes > config.getMemtableThresholdBytes()) {
                flushMemtable();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist data", e);
        }
    }
    
    /**
     * Performs a logical delete of a key. A special "tombstone" marker is written to the WAL
     * and the memtable. This ensures that the key is considered deleted in subsequent reads,
     * and the record will be physically removed during a future compaction.
     *
     * @param key The key to delete.
     * @throws RuntimeException if the write to the WAL fails.
     */
    @Override
    public void delete(String key) {
        try {
            LogEntry tombstone = new LogEntry(key, LogEntry.TOMBSTONE);
            wal.writeEntry(tombstone);
            metrics.walWrites.increment();
            memtable.put(key, LogEntry.TOMBSTONE);
            memtableSizeInBytes += key.length();

            if (memtableSizeInBytes > config.getMemtableThresholdBytes()) {
                flushMemtable();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist delete", e);
        }
    }

    /**
     * Triggers an asynchronous flush of the current memtable to a new SSTable on disk.
     * This method swaps the active memtable with a new empty one and submits a background
     * task to handle the I/O-intensive process of writing the SSTable file. Once the SSTable is
     * written, the corresponding WAL segment is safely deleted.
     */
    private void flushMemtable() {
        if (memtable.isEmpty()) return;
        metrics.memtableFlushes.increment();
        try {
            final Path retiredWalSegment = wal.rollNewSegment();
            final ConcurrentNavigableMap<String, String> oldMemtable = this.memtable;
            this.memtable = new ConcurrentSkipListMap<>();
            this.memtableSizeInBytes = 0;
            flushExecutor.submit(() -> {
                try {
                    Path sstPath = sstableWriter.write(oldMemtable);
                    if (sstPath != null) {
                        sstableManager.addNewSSTable(sstPath);
                    }
                    if (retiredWalSegment != null) {
                        Files.delete(retiredWalSegment);
                    }
                } catch (IOException e) {
                    logger.error("Background flush failed", e);
                }
            });

        } catch (IOException e) {
            logger.error("Failed to flush Memtable", e);
        }
    }

    /**
     * Retrieves the value associated with a given key.
     * The search follows the read path of an LSM-Tree:
     * 1. Check the memtable for the most recent value.
     * 2. If not found, search the SSTables on disk, from newest to oldest.
     *
     * If a tombstone is found for the key, it is treated as if the key does not exist.
     *
     * @param key The key to look up.
     * @return An {@link Optional} containing the value if the key is found, otherwise an empty {@code Optional}.
     */
   @Override
    public Optional<String> get(String key) {
        long startTime = System.nanoTime(); 
        metrics.gets.increment();
        String memtableValue = memtable.get(key);
        if (memtableValue != null) {
            metrics.recordGetLatency(System.nanoTime() - startTime);
            return memtableValue == LogEntry.TOMBSTONE ? Optional.empty() : Optional.of(memtableValue);
        }

        Optional<LogEntry> sstableEntry = sstableManager.find(key);
        metrics.recordGetLatency(System.nanoTime() - startTime);
        if (sstableEntry.isPresent()) {
            LogEntry entry = sstableEntry.get();
            return entry.isTombstone() ? Optional.empty() : Optional.of(entry.getValue());
        }
        
        return Optional.empty();
    }
}