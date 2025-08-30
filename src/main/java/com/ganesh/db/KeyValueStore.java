package com.ganesh.db;

import java.io.IOException;
import java.util.Optional;

/**
 * Defines the public API for a persistent key-value store.
 * Implementations of this interface provide mechanisms to store, retrieve,
 * and delete string-based key-value pairs.
 */
public interface KeyValueStore {

    /**
     * Starts the key-value store. This method must be called before any other operations.
     * It handles loading data from disk and preparing the store for use.
     * @throws IOException if there is an error during initialization, such as reading from disk.
     */
    void start() throws IOException;

    /**
     * Stops the key-value store, ensuring all data is flushed and resources are released.
     * No operations should be called after the store is stopped.
     */
    void stop();

    /**
     * Inserts or updates a key-value pair into the store.
     *
     * @param key The key for the data. Must not be null.
     * @param value The value to be stored. Must not be null.
     */
    void put(String key, String value);

    /**
     * Deletes a key and its associated value from the store.
     * If the key does not exist, this operation has no effect.
     *
     * @param key The key to delete. Must not be null.
     */
    void delete(String key);

    /**
     * Retrieves a value by its key.
     *
     * @param key The key to look up. Must not be null.
     * @return An {@link Optional} containing the value if the key is found,
     * or an empty Optional if the key does not exist.
     */
    Optional<String> get(String key);

    /**
     * Retrieves the metrics collector for this store instance.
     *
     * @return The {@link StoreMetrics} object containing performance counters and statistics.
     */
    StoreMetrics getMetrics();
}