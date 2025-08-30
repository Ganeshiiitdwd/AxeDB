package com.ganesh.db.sstable;

import com.ganesh.db.StoreMetrics;
import com.ganesh.db.wal.LogEntry;
import com.google.common.hash.BloomFilter; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

/**
 * Provides read access to a single SSTable file.
 *
 * <p>This class serves two primary purposes:
 * <ol>
 * <li><b>Efficient Key Lookups:</b> The {@link #find(String, StoreMetrics)} method allows for fast,
 * indexed searches for a specific key.</li>
 * <li><b>Sequential Scanning:</b> It implements the {@link Iterator} interface, enabling a full,
 * ordered scan of all key-value pairs, which is essential for the compaction process.</li>
 * </ol>
 *
 * Upon initialization, it reads the SSTable's footer to load the Bloom filter and the entire
 * sparse index into memory. This metadata allows for lookups that minimize disk I/O.
 * The class also implements {@link Closeable} to ensure the underlying file handle is released.
 *
 * @see SSTableWriter
 */
public class SSTableReader implements Iterator<LogEntry>, Closeable {
   private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);
    private final RandomAccessFile raf;
    private final Path filePath;
    private LogEntry nextEntry;
    private  long indexOffset;
    private final List<IndexRecord> index;
    private final long timestamp;
    private final BloomFilter<String> bloomFilter;

    /**
     * An in-memory representation of a single entry in the SSTable's sparse index.
     */
    private static class IndexRecord {
        /** The last key within a given data block. */
        final String key;
        /** The starting byte offset of that data block in the file. */
        final long offset;
        IndexRecord(String key, long offset) { this.key = key; this.offset = offset; }
        String getKey() { return key; }
    }

    /**
     * Opens an SSTable file for reading and loads its metadata (index and Bloom filter) into memory.
     *
     * @param filePath The path to the {@code .sst} file.
     * @throws IOException If the file cannot be opened, is malformed, or an I/O error occurs.
     */
     public SSTableReader(Path filePath) throws IOException {
        this.filePath = filePath;
        this.raf = new RandomAccessFile(filePath.toFile(), "r");
        this.index = new ArrayList<>();

        String fileName = filePath.getFileName().toString();
        this.timestamp = Long.parseLong(fileName.replace(".sst", ""));

        if (this.raf.length() < 16) { 
            throw new IOException("Invalid SSTable file (too small for footer): " + filePath);
        }

        this.bloomFilter = loadMetadata();

        this.raf.seek(0);
        loadNext();
    }

    /**
     * Reads the SSTable footer to locate, deserialize, and load the Bloom filter and sparse index.
     *
     * @return The deserialized {@link BloomFilter} for this SSTable.
     * @throws IOException if an I/O error occurs while reading the metadata.
     */
    private BloomFilter<String> loadMetadata() throws IOException {
        raf.seek(raf.length() - 16);
        long indexOffset = raf.readLong();
        long bloomFilterOffset = raf.readLong();
        this.indexOffset = indexOffset;

        // Load Bloom Filter
        raf.seek(bloomFilterOffset);
        BloomFilter<String> loadedFilter = BloomFilter.readFrom(
                new BufferedInputStream(new FileInputStream(raf.getFD())),
                LogEntry.KEY_FUNNEL
        );

        // Load Index
        raf.seek(indexOffset);
        while (raf.getFilePointer() < bloomFilterOffset) { // Index ends where bloom filter begins
            String key = raf.readUTF();
            long offset = raf.readLong();
            this.index.add(new IndexRecord(key, offset));
        }

        return loadedFilter;
    }

    /**
     * Returns the next entry that the iterator will return, without advancing the iterator.
     *
     * @return The next {@link LogEntry}, or {@code null} if the iteration has finished.
     */
    public LogEntry peek() {
        return this.nextEntry;
    }

    /**
     * Returns the timestamp of the SSTable, derived from its filename.
     * This is used to determine the age of the file relative to others.
     *
     * @return The timestamp as a long.
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * Pre-loads the next available {@link LogEntry} from the data section of the file.
     * Sets the internal {@code nextEntry} to {@code null} if the end of the data blocks is reached.
     *
     * @throws IOException if an I/O error occurs during reading.
     */
    private void loadNext() throws IOException {
        try {
            if (raf.getFilePointer() >= this.indexOffset) {
                this.nextEntry = null;
                return;
            }
            this.nextEntry = LogEntry.readFrom(raf);
        } catch (EOFException e) {
            this.nextEntry = null;
        }
    }

    /**
     * Efficiently finds a key within the SSTable using the Bloom filter and sparse index.
     * <p>
     * The lookup process is as follows:
     * 1. Check the Bloom filter. If it indicates the key is not present, return immediately.
     * 2. Perform a binary search on the in-memory index to locate the data block that may contain the key.
     * 3. Read only that specific data block from disk into memory.
     * 4. Perform a linear scan within the small data block to find the exact key.
     *
     * @param key     The key to search for.
     * @param metrics The metrics collector to update with operational stats.
     * @return An {@link Optional} containing the {@link LogEntry} if found, otherwise an empty {@code Optional}.
     * @throws IOException if a disk I/O error occurs.
     */
    public Optional<LogEntry> find(String key, StoreMetrics metrics) throws IOException {
        metrics.bloomFilterChecks.increment();
        if (!bloomFilter.mightContain(key)) {
            metrics.bloomFilterHits.increment();
            return Optional.empty(); // Definitely not in this file, a "quick no".
        }
        // Use binary search on the sparse index to find the right block
        int blockIndex = Collections.binarySearch(index, new IndexRecord(key, 0), Comparator.comparing(IndexRecord::getKey));
        if (blockIndex < 0) {
            // The key would be in the block at this insertion point
            blockIndex = -blockIndex - 1;
        }
        
        if (blockIndex >= index.size()) {
            logger.debug("Key '{}' not found, search key is past all block ranges in {}.", key, filePath.getFileName());
            return Optional.empty();
        }

        // Determine the boundaries of the block to read
        IndexRecord blockInfo = index.get(blockIndex);
        long blockStartOffset = blockInfo.offset;
        long blockEndOffset = (blockIndex + 1 < index.size()) ? index.get(blockIndex + 1).offset : this.indexOffset;

        logger.debug("Key '{}' search: Using index to read block {} at offset {} in {}.", key, blockIndex, blockStartOffset, filePath.getFileName());
        raf.seek(blockStartOffset);
        // Read the entire block into memory and scan it
        int blockSize = (int) (blockEndOffset - blockStartOffset);
        byte[] blockData = new byte[blockSize];
        raf.readFully(blockData);

        try (DataInputStream blockStream = new DataInputStream(new ByteArrayInputStream(blockData))) {
            while (blockStream.available() > 0) {
                LogEntry entry = LogEntry.readFrom(blockStream);
                int comparison = entry.getKey().compareTo(key);
                if (comparison == 0) return Optional.of(entry);
                if (comparison > 0) return Optional.empty();
            }
        }
        
        return Optional.empty();
    }

    /**
     * Returns the path of the file this reader is associated with.
     * @return The file {@link Path}.
     */
    public Path getFilePath() {
        return filePath;
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

     @Override
    public LogEntry next() {
        if (!hasNext()) throw new NoSuchElementException();
        LogEntry current = nextEntry;
        try {
            loadNext();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next entry from SSTable", e);
        }
        return current;
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
        }
    }
}