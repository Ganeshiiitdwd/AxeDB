package com.ganesh.db.sstable;

import com.ganesh.db.wal.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * Provides the logic for merging multiple SSTables into a single, consistent dataset.
 *
 * <p>The primary role of this class is to execute the core merge step of the compaction process.
 * It takes a collection of SSTable readers and uses a k-way merge algorithm to produce a unified,
 * sorted map. This process is essential for reducing the number of SSTable files, improving
 * read performance, and physically removing data that is obsolete or marked for deletion (tombstoned).
 */
public class Compactor {
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);
    /**
     * Merges multiple sorted SSTables into a single, sorted map containing only the most recent
     * version of each key.
     *
     * <p>This method implements a k-way merge algorithm using a {@link PriorityQueue} to efficiently
     * process entries from all SSTables simultaneously. The priority queue is ordered to ensure that
     * for any given key, the entry from the newest SSTable (highest timestamp) is always processed first.
     *
     * <p>The logic iterates through all entries, discarding older versions of a key and removing
     * any key whose most recent entry is a tombstone.
     *
     * @param readersToCompact A list of {@link SSTableReader} instances for the files to be merged.
     * @return A {@link LinkedHashMap} containing the merged, sorted, and cleaned data,
     * ready to be written to a new, compacted SSTable. The map preserves the key sort order.
     */
    public Map<String, String> merge(List<SSTableReader> readersToCompact) {
        logger.info("Merging data from {} SSTables into memory.", readersToCompact.size());

        PriorityQueue<SSTableReader> pq = new PriorityQueue<>(
            Comparator.comparing((SSTableReader r) -> r.peek().getKey())
                      .thenComparing(SSTableReader::getTimestamp, Comparator.reverseOrder())
        );

        for (SSTableReader reader : readersToCompact) {
            if (reader.hasNext()) {
                pq.add(reader);
            }
        }

        Map<String, String> mergedResults = new LinkedHashMap<>();
        String lastKey = null;

        while (!pq.isEmpty()) {
            SSTableReader reader = pq.poll();
            LogEntry entry = reader.next(); 

            if (!entry.getKey().equals(lastKey)) {
                lastKey = entry.getKey();
                if (!entry.isTombstone()) {
                    mergedResults.put(entry.getKey(), entry.getValue());
                }
            }

            if (reader.hasNext()) {
                pq.add(reader);
            }
        }
        return mergedResults;
    }

}