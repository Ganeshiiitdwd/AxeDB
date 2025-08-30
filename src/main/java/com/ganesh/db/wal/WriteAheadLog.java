package com.ganesh.db.wal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

/**
 * Implements a Write-Ahead Log (WAL) for ensuring data durability and crash recovery.
 *
 * <p>The WAL provides durability by writing all database operations (put/delete) to a persistent log on disk
 * before they are applied to the in-memory data structures. In case of a crash, the log can be replayed
 * to restore the database to its last known state.
 *
 * <p>This implementation uses log segmentation, where logs are written to timestamped files. When a log
 * segment is no longer needed (i.e., its corresponding memtable has been flushed to an SSTable),
 * it can be safely deleted. This class implements {@link AutoCloseable} for proper resource management.
 *
 * @see LogEntry
 */
public class WriteAheadLog implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    /**
     * The file extension for WAL segment files.
    */
    public static final String WAL_FILE_EXTENSION = ".wal";
    /**
     * The directory where WAL segment files are stored.
    */
    private static String dataDirectory;
    private Path currentLogFilePath;
    private DataOutputStream logWriter;
    private FileOutputStream fileOutputStream;

    private static final int SYNC_INTERVAL = 1000;
    private int writesSinceLastSync = 0;
    /**
     * Constructs a new WriteAheadLog and initializes the first log segment.
     *
     * @param dataDirectory The directory where log files will be stored.
     * @throws IOException If an I/O error occurs while creating the initial log segment.
     */
    @SuppressWarnings("static-access")
    public WriteAheadLog(String dataDirectory) throws IOException {
        this.dataDirectory = dataDirectory;
        rollNewSegment(); // Start with a fresh segment
    }

    /**
     * Closes the current log segment file and opens a new one.
     * This is typically called when the memtable is being flushed to disk. The old segment
     * can then be safely removed once the flush is complete.
     *
     * @return The {@link Path} to the log segment that was just closed (the "retired" segment).
     * @throws IOException If an I/O error occurs during the process.
     */
    public Path rollNewSegment() throws IOException {
        Path retiredSegmentPath = null;
        if (logWriter != null) {
            close(); // Close the current writer
            retiredSegmentPath = this.currentLogFilePath;
        }

        this.currentLogFilePath = Paths.get(dataDirectory, System.currentTimeMillis() + WAL_FILE_EXTENSION);
        // Open in append mode and get the underlying stream for forced flushes.
        this.fileOutputStream = new FileOutputStream(currentLogFilePath.toFile(), true); 
        this.logWriter = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        logger.info("Rolled to new WAL segment: {}", currentLogFilePath);
        return retiredSegmentPath;
    }

    /**
     * Writes a {@link LogEntry} to the log, using a batched syncing strategy for high performance.
     * The data is written to the OS buffer immediately, but the physical disk sync is only
     * forced periodically based on {@link #SYNC_INTERVAL}.
     *
     * @param entry The log entry to persist.
     * @throws IOException If the write operation fails.
     */
    public void writeEntry(LogEntry entry) throws IOException {
        entry.writeTo(logWriter);
        writesSinceLastSync++;

        if (writesSinceLastSync >= SYNC_INTERVAL) {
            logWriter.flush();
            // Force syncs data and metadata to the underlying storage device for durability.
            fileOutputStream.getChannel().force(true);
            writesSinceLastSync = 0;
        }
    }
    
    /**
     * Replays all WAL segments to reconstruct the in-memory state.
     * <p>
     * This static method is called on database startup. It finds all files with the {@code .wal} extension
     * in the data directory, sorts them chronologically by their timestamped names, and applies each
     * logged operation to the provided map.
     *
     * @param map The map (typically the memtable) to populate with the recovered data.
     * @throws IOException If an error occurs while reading the WAL files.
     */
    public static void replayAll(Map<String, String> map) throws IOException {
        logger.info("Starting WAL recovery...");
        File dataDir = new File(dataDirectory);
        if (!dataDir.exists()) return;

        File[] walFiles = dataDir.listFiles((dir, name) -> name.endsWith(WAL_FILE_EXTENSION));

        if (walFiles == null || walFiles.length == 0) {
            logger.info("No WAL files found. No recovery needed.");
            return;
        }

        // Sort files chronologically based on their timestamped names
        Arrays.sort(walFiles);

        int totalRecords = 0;
        for (File walFile : walFiles) {
            logger.info("Replaying WAL segment: {}", walFile.getName());
            try (FileInputStream fis = new FileInputStream(walFile);
                 DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {

                while (dis.available() > 0) {
                    try {
                        LogEntry entry = LogEntry.readFrom(dis);
                        map.put(entry.getKey(), entry.getValue());
                        totalRecords++;
                    } catch (EOFException e) {
                        logger.warn("Reached end of file unexpectedly in {}. WAL may be partially written.", walFile.getName());
                        break;
                    }
                }
            }
        }
        logger.info("WAL recovery complete. Replayed {} records from {} segment(s).", totalRecords, walFiles.length);
    }

    /**
     * Closes the underlying log stream. This is automatically called when used in a try-with-resources statement.
     *
     * @throws IOException If an I/O error occurs when closing the stream.
     */
    @Override
    public void close() throws IOException {
        if (logWriter != null) {
            logWriter.close();
        }
    }
}