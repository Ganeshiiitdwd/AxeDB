package com.ganesh.db.sstable;

import com.ganesh.db.Config;
import com.ganesh.db.wal.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.hash.BloomFilter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles the process of writing an in-memory Memtable to a persistent, immutable SSTable file on disk.
 *
 * <p>This class implements a crash-safe writing strategy by first writing all data to a temporary
 * {@code .tmp} file. Only upon successful completion is the temporary file atomically renamed to its
 * final {@code .sst} destination. This prevents corrupted or partially written files from being
* recognized by the database after a crash.
 *
 * <p>The SSTable file format created by this writer consists of:
 * <ol>
 * <li><b>Data Blocks:</b> Contiguous blocks of serialized key-value pairs ({@link LogEntry}).</li>
 * <li><b>Index Block:</b> A sparse index mapping the last key of each data block to its starting offset in the file.</li>
 * <li><b>Bloom Filter:</b> A serialized Bloom filter for fast, probabilistic key existence checks.</li>
 * <li><b>Footer:</b> A fixed-size section at the end of the file containing the offsets of the index block and the Bloom filter.</li>
 * </ol>
 */
public class SSTableWriter {
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);
    private final Config config;

    /**
     * Constructs an SSTableWriter with the given database configuration.
     *
     * @param config The configuration object, used for settings like data directory and block sizes.
     */
    public SSTableWriter(Config config) { // SPRINT 7 CHANGE
        this.config = config;
    }
    
    /**
     * Represents a single entry in the SSTable's sparse index.
     */
    private static class IndexEntry {
        /** The last key of a data block. */
        final String key;
        /** The starting byte offset of that data block within the SSTable file. */
        final long offset;

        IndexEntry(String key, long offset) {
            this.key = key;
            this.offset = offset;
        }

        /**
         * Serializes the index entry to the provided output stream.
         * @param out The stream to write to.
         * @throws IOException if an I/O error occurs.
         */
        void writeTo(DataOutputStream out) throws IOException {
            out.writeUTF(key);
            out.writeLong(offset);
        }
    }

    /**
     * Writes the contents of a given Memtable to a new SSTable file.
     *
     * @param memtable The sorted map of key-value pairs to be persisted.
     * @return The {@link Path} of the newly created SSTable file, or {@code null} if the memtable was empty.
     * @throws IOException if any file I/O error occurs during the write process.
     */

    public Path write(Map<String, String> memtable) throws IOException {
        if (memtable.isEmpty()) {
            return null;
        }

        long timestamp = System.currentTimeMillis();
        String finalSstFileName = timestamp + ".sst";
        String tempSstFileName = timestamp + ".tmp";

        Path tempFile = Paths.get(config.getDataDirectory(), tempSstFileName);
        Path finalFile = Paths.get(config.getDataDirectory(), finalSstFileName);

        logger.info("Flushing Memtable to temporary SSTable file: {}", tempFile);
        BloomFilter<String> bloomFilter = BloomFilter.create(
                LogEntry.KEY_FUNNEL,
                memtable.size(),
                config.getBloomFilterFpp()
        );
        List<IndexEntry> index = new ArrayList<>(); 
        long currentOffset = 0;

        try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
             DataOutputStream out = new DataOutputStream(fos)) {

            ByteArrayOutputStream blockBuffer = new ByteArrayOutputStream();
            DataOutputStream blockOut = new DataOutputStream(blockBuffer);
            String lastKeyInBlock = null;
            // Step 1: Write all key-value pairs in data blocks
            for (Map.Entry<String, String> entry : memtable.entrySet()) { 
                String key = entry.getKey();
                bloomFilter.put(key);
                LogEntry logEntry = new LogEntry(entry.getKey(), entry.getValue());
                logEntry.writeTo(blockOut);
                lastKeyInBlock = entry.getKey();

                if (blockBuffer.size() >= config.getSstableBlockSizeBytes()) {
                    index.add(new IndexEntry(lastKeyInBlock, currentOffset));
                    byte[] blockData = blockBuffer.toByteArray();
                    out.write(blockData);
                    currentOffset += blockData.length;
                    blockBuffer.reset();
                }
            }

            // Flush any remaining data in the last block
            if (blockBuffer.size() > 0) {
                index.add(new IndexEntry(lastKeyInBlock, currentOffset));
                byte[] blockData = blockBuffer.toByteArray();
                out.write(blockData);
                currentOffset += blockData.length;
            }

            // Step 2: Write the index block
            long indexOffset = currentOffset;
            for (IndexEntry indexEntry : index) {
                indexEntry.writeTo(out);
            }

            // Step 3: Write the Bloom filter
            long bloomFilterOffset = out.size();
            bloomFilter.writeTo(out);

            // Step 4: Write the footer with offsets for metadata
            out.writeLong(indexOffset);
            out.writeLong(bloomFilterOffset);
        }

         // Step 5: Atomically move the temp file to its final destination
        logger.info("Memtable flush successful. Renaming {} to {}", tempFile, finalFile);
        Files.move(tempFile, finalFile, StandardCopyOption.ATOMIC_MOVE);
        return finalFile;
    }
}