package com.ganesh.db;

/**
 * Holds the configuration for a {@link DurableStore} instance.
 * Use the nested {@link Builder} class to construct a configuration object.
 */
public class Config {
    private final String dataDirectory;
    private final long memtableThresholdBytes;
    private final int compactionTriggerFileCount;
    private final int sstableBlockSizeBytes;
    private final double bloomFilterFpp;

    private Config(Builder builder) {
        this.dataDirectory = builder.dataDirectory;
        this.memtableThresholdBytes = builder.memtableThresholdBytes;
        this.compactionTriggerFileCount = builder.compactionTriggerFileCount;
        this.sstableBlockSizeBytes = builder.sstableBlockSizeBytes;
        this.bloomFilterFpp = builder.bloomFilterFpp;
    }

    // Getters for all configuration properties
    public String getDataDirectory() { return dataDirectory; }
    public long getMemtableThresholdBytes() { return memtableThresholdBytes; }
    public int getCompactionTriggerFileCount() { return compactionTriggerFileCount; }
    public int getSstableBlockSizeBytes() { return sstableBlockSizeBytes; }
    public double getBloomFilterFpp() { return bloomFilterFpp; }

    /**
     * A builder for creating immutable {@link Config} instances.
     * Provides default values for all configuration parameters.
     */
    public static class Builder {
        private String dataDirectory = "data/";
        private long memtableThresholdBytes = 1024 * 1024; // 1 MB
        private int compactionTriggerFileCount = 4;
        private int sstableBlockSizeBytes = 4 * 1024; // 4 KB
        private double bloomFilterFpp = 0.01; // 1% false positive probability

        /**
         * Sets the directory where data files (WAL, SSTables) will be stored.
         * @param dataDirectory The path to the data directory.
         * @return This builder instance for chaining.
         */
        public Builder withDataDirectory(String dataDirectory) {
            this.dataDirectory = dataDirectory;
            return this;
        }

        public Builder withMemtableThresholdBytes(long memtableThresholdBytes) {
            this.memtableThresholdBytes = memtableThresholdBytes;
            return this;
        }

        public Builder withCompactionTriggerFileCount(int count) {
            this.compactionTriggerFileCount = count;
            return this;
        }

        public Builder withSstableBlockSizeBytes(int bytes) {
            this.sstableBlockSizeBytes = bytes;
            return this;
        }

        public Builder withBloomFilterFpp(double fpp) {
            this.bloomFilterFpp = fpp;
            return this;
        }


        /**
         * Builds the final, immutable {@link Config} object.
         * @return A new Config instance.
         */
        public Config build() {
            return new Config(this);
        }
    }
}