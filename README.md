AxeDB
===========================================

This project is a high-performance, persistent key-value store built from scratch in Java. It is architected as a Log-Structured Merge-Tree (LSM-Tree), inspired by the storage engines of production databases like RocksDB and Apache Cassandra.

The engine is designed for high write throughput and efficient storage management, featuring crash-safe durability, background data compaction, and optimized read paths.

🚀 Performance Highlights
-------------------------

The storage engine was benchmarked on a consumer-grade laptop (Ryzen 5, 8GB RAM) to measure its peak performance under various workloads.

## 📊 Performance Metrics

| Metric                  | Result                  | Description                                                                 |
|--------------------------|-------------------------|-----------------------------------------------------------------------------|
| **Operational Throughput** | **1 Million+ ops/sec** | The maximum rate of write operations the engine can process.                |
| **Data Throughput**       | **450+ MB/sec**        | The maximum sustained data ingestion rate for large values.      |
| **Space Reclamation**     | **57% Disk Usage Reduction** | Space recovered by the compaction process after heavy updates and deletes. |


✨ Key Features
--------------

The project is a complete implementation of a modern storage engine, including:

*   **Log-Structured Merge-Tree (LSM-Tree) Architecture:** All writes are directed to an in-memory Memtable and then flushed to immutable SSTable files on disk, optimizing for high write throughput.
    
*   **Write-Ahead Log (WAL):** Guarantees durability and crash recovery. All operations are first written to a batched, synchronous log before being applied in memory.
    
*   **Memtable & SSTables:** A ConcurrentSkipListMap acts as the in-memory write buffer (Memtable). When full, it is flushed to a Sorted String Table (SSTable) file.
    
*   **Background Compaction:** A multi-threaded background process merges smaller SSTables into larger ones, reclaiming space from overwritten and deleted data and improving read performance.
    
*   **Sparse Indexing:** Each SSTable contains a sparse index of its data blocks, allowing for fast lookups without reading the entire file into memory.
    
*   **Bloom Filters:** Each SSTable includes a Bloom filter, enabling extremely fast, probabilistic checks for non-existent keys to avoid unnecessary disk I/O.
    

🛠️ Tech Stack
--------------

*   **Language:** Java (JDK 11)
    
*   **Build & Dependencies:** Apache Maven
    
*   **Libraries:**
    
    *   **Google Guava:** For Bloom Filter implementation.
        
    *   **SLF4J & Logback:** For structured logging.
        
    *   **JUnit 5:** For unit, integration, and performance benchmark testing.
        

🔧 How to Build and Run
-----------------------

### Prerequisites

*   Java Development Kit (JDK) 11 or later
    
*   Apache Maven
    

### Build the Project

Clone the repository and run the following Maven command from the project's root directory. This will compile the source code and package it into a JAR file.
```bash
   mvn clean install
```

### Run the Benchmarks

All performance benchmarks are integrated as JUnit tests. To run the full suite of benchmarks (Operational Throughput, Data Throughput, Space Reclamation, and Read Performance), execute:
```bash
   mvn test   
```
The results of each benchmark will be printed to the console in a formatted table.

🏛️ Architectural Overview
--------------------------

### Write Path

A write operation follows a clear, durability-first path:

1.  **Write-Ahead Log:** The key-value pair is first appended to the WAL file on disk, guaranteeing the operation will survive a crash.
    
2.  **Memtable:** After a successful WAL write, the key-value pair is inserted into the in-memory Memtable.
    
3.  **Flush to SSTable:** When the Memtable reaches a size threshold, it is "frozen," and its contents are written to a new, immutable SSTable file on disk.
    

### Read Path

A read operation checks for the key in order of recency to find the most up-to-date value:

1.  **Memtable:** The in-memory Memtable is checked first.
    
2.  **SSTables (Newest to Oldest):**
    
    *   **Bloom Filter:** The Bloom filter for each SSTable is checked. If it indicates the key is **not** present, the search for that file is skipped entirely.
        
    *   **Sparse Index:** If the Bloom filter passes, the in-memory sparse index is used to quickly identify which data block _might_ contain the key.
        
    *   **Data Block:** Only the identified data block is read from disk and scanned for the key.
