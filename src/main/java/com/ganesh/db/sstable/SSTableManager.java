package com.ganesh.db.sstable;

import com.ganesh.db.Config;
import com.ganesh.db.StoreMetrics;
import com.ganesh.db.wal.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the complete lifecycle of all SSTable files on disk.
 *
 * <p>This class is the central coordinator for all operations involving SSTables. Its responsibilities include:
 * <ul>
 * <li>Loading existing SSTables from the data directory on startup.</li>
 * <li>Coordinating read operations (lookups) across all active SSTables.</li>
 * <li>Registering new SSTables after a Memtable is flushed.</li>
 * <li>Orchestrating the background compaction process to merge SSTables.</li>
 * </ul>
 *
 * <p>Thread safety is managed using a {@link ReentrantReadWriteLock} to allow for concurrent reads
 * while ensuring that modifications to the list of active SSTables (like adding a new one or during
 * compaction) are performed with exclusive access.
 */
public class SSTableManager {
    private static final Logger logger = LoggerFactory.getLogger(SSTableManager.class);

    /** A thread-safe list of active SSTable readers, sorted from newest to oldest. Access is guarded by {@link #lock}. */
    private final List<SSTableReader> sstableReaders = new ArrayList<>();
    /** A read-write lock to ensure safe concurrent access to the {@link #sstableReaders} list. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    /** A single-threaded executor to run compaction tasks in the background, ensuring serial execution. */
    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor();
     /** The helper class responsible for the k-way merge logic of compaction. */
    private final Compactor compactor = new Compactor();
    /** An atomic flag to prevent scheduling a new compaction while one is already in progress. */
    private final AtomicBoolean isCompacting = new AtomicBoolean(false);
    private final Config config;
    private final StoreMetrics metrics;

    /**
     * Constructs an SSTableManager.
     *
     * @param config  The database configuration object.
     * @param metrics The metrics collector for tracking performance.
     */
    public SSTableManager(Config config, StoreMetrics metrics) { 
        this.config = config;
        this.metrics = metrics; 
    }

    /**
     * Scans the data directory on startup, loading all existing {@code .sst} files into memory.
     * Files are sorted chronologically from newest to oldest based on their timestamped filenames.
     *
     * @throws IOException If an error occurs while creating a reader for an SSTable file.
     */
    public void loadSSTables() throws IOException {
        File dataDir = new File(config.getDataDirectory());
        if (!dataDir.exists()) return;

        File[] sstFiles = dataDir.listFiles((dir, name) -> name.endsWith(".sst"));
        if (sstFiles == null) return;

        Arrays.sort(sstFiles, Comparator.comparing(File::getName).reversed());

        for (File sstFile : sstFiles) {
            logger.info("Loading SSTable: {}", sstFile.getName());
            sstableReaders.add(new SSTableReader(sstFile.toPath()));
        }
    }

    /**
     * Searches for a key across all managed SSTables, from newest to oldest.
     * The search stops as soon as the key is found, which correctly retrieves the most recent
     * value (or tombstone) for that key. This method is thread-safe for concurrent reads.
     *
     * @param key The key to search for.
     * @return An {@link Optional} containing the {@link LogEntry} if found, otherwise an empty {@code Optional}.
     */
    public Optional<LogEntry> find(String key) {
        lock.readLock().lock();
        try {
            for (SSTableReader reader : sstableReaders) {
                try {
                    Optional<LogEntry> entryOpt = reader.find(key,metrics);
                    if (entryOpt.isPresent()) {
                        return entryOpt;
                    }
                } catch (IOException e) {
                    logger.error("Error reading from SSTable", e);
                }
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Adds a new SSTable to the manager, typically after a Memtable flush.
     * The new table is added to the front of the list to maintain the newest-to-oldest order.
     * After adding, it checks if a compaction should be triggered.
     *
     * @param sstPath The path of the new SSTable file.
     * @throws IOException If a reader for the new file cannot be created.
     */
    public void addNewSSTable(Path sstPath) throws IOException {
        lock.writeLock().lock();
        try {
            sstableReaders.add(0, new SSTableReader(sstPath));
        } finally {
            lock.writeLock().unlock();
        }
        scheduleCompactionIfNeeded();
    }

    /**
     * Checks if conditions are met to start a new compaction task.
     * A compaction is scheduled if the number of SSTables exceeds the configured threshold
     * and a compaction is not already in progress.
     */
    private void scheduleCompactionIfNeeded() {
        if (sstableReaders.size() >= config.getCompactionTriggerFileCount() && isCompacting.compareAndSet(false, true)) {
            compactionExecutor.submit(this::runCompaction);
        }
    }
    
    /**
     * Executes the compaction process on a background thread. This method acquires a write lock
     * to ensure the list of active SSTables is not modified during the process.
     *
     * The process is designed to be crash-safe:
     * 1. Merges all data from the old readers into a single in-memory map.
     * 2. Closes the old readers to release file handles.
     * 3. Writes the merged data to a new SSTable file.
     * 4. Atomically replaces the old list of readers with a new list containing only the new reader.
     * 5. Deletes the old, now obsolete SSTable files.
     */
     private void runCompaction() {
        metrics.compactions.increment(); 
        lock.writeLock().lock();
        try {
            if (sstableReaders.size() < config.getCompactionTriggerFileCount()) {
                return;
            }

            List<SSTableReader> readersToCompact = new ArrayList<>(sstableReaders);
            List<Path> pathsToDelete = new ArrayList<>();
            for(SSTableReader reader : readersToCompact) {
                pathsToDelete.add(reader.getFilePath());
            }

            Map<String, String> mergedData = compactor.merge(readersToCompact);

            for (SSTableReader reader : readersToCompact) {
                reader.close();
            }

            SSTableWriter writer = new SSTableWriter(config);
            Path newSSTablePath = writer.write(mergedData);

            SSTableReader newReader = (newSSTablePath != null) ? new SSTableReader(newSSTablePath) : null;
            
            sstableReaders.clear();
            if (newReader != null) {
                sstableReaders.add(newReader);
            }

            for (Path path : pathsToDelete) {
                Files.delete(path);
            }
            logger.info("Compaction complete. Old files deleted.");

        } catch (IOException e) {
            logger.error("Compaction failed", e);
        } finally {
            isCompacting.set(false);
            lock.writeLock().unlock();
        }
    }

    /**
     * Gracefully shuts down the SSTableManager. This includes terminating the compaction
     * executor and closing all open file handles held by the SSTable readers.
     *
     * @throws IOException If closing a reader fails.
     */
    public void close() throws IOException {
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("Compaction executor did not terminate in the allotted time.");
            }
        } catch (InterruptedException e) {
            logger.error("Compaction executor shutdown was interrupted.", e);
            Thread.currentThread().interrupt();
        }

        for (SSTableReader reader : sstableReaders) {
            reader.close();
        }
    }
}