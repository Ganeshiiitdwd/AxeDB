package com.ganesh.db;

import com.ganesh.db.wal.WriteAheadLog;
import com.ganesh.db.sstable.SSTableManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unused")
class DurableStoreTest {

    private DurableStore store;
    private Config config;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        this.config = new Config.Builder()
                .withDataDirectory(tempDir.resolve("test_data").toString())
                .withMemtableThresholdBytes(1024) 
                .withCompactionTriggerFileCount(4)
                .build();
        File dataDir = new File(config.getDataDirectory());
        // ... delete directory logic ...
        dataDir.mkdirs();

        store = new DurableStore(config);
        store.start();
    }
    @AfterEach
    void tearDown() {
        if (store != null) {
            store.stop();
        }
    }

    @Test
    void testPutAndGet() {
        store.put("user1", "Ganesh");
        Optional<String> result = store.get("user1");
        assertTrue(result.isPresent());
        assertEquals("Ganesh", result.get());
    }

    @Test
    void testGetNonExistentKey() {
        Optional<String> result = store.get("nonexistent");
        assertFalse(result.isPresent());
    }
    
    @Test
    void testUpdateValue() {
        store.put("key1", "value1");
        store.put("key1", "value2");
        Optional<String> result = store.get("key1");
        assertTrue(result.isPresent());
        assertEquals("value2", result.get());
    }

     @Test
    void testRecoveryAfterRestart() throws IOException {
        store.put("key1", "value1");
        store.put("key2", "value2");
        store.stop(); 

        KeyValueStore newStore = new DurableStore(config);
        newStore.start();

        assertEquals("value1", newStore.get("key1").orElse(null));
        assertEquals("value2", newStore.get("key2").orElse(null));
        newStore.stop();
        
        this.store = null; 
    }

    @Test
    void testMemtableFlush() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            store.put("key" + i, "a_long_value_to_make_sure_we_cross_the_threshold" + i);
        }
        
        Thread.sleep(500);

        File dataDir = new File(config.getDataDirectory());
        File[] sstFiles = dataDir.listFiles((dir, name) -> name.endsWith(".sst"));
        assertNotNull(sstFiles);
        assertTrue(sstFiles.length > 0, "At least one SSTable should have been created");
    }


   @Test
    void testWalIsDeletedAfterFlush() throws InterruptedException {
        File dataDir = new File(config.getDataDirectory());
        File[] initialWalFiles = dataDir.listFiles((dir, name) -> name.endsWith(WriteAheadLog.WAL_FILE_EXTENSION));
        assertNotNull(initialWalFiles);
        assertEquals(1, initialWalFiles.length);
        String initialWalName = initialWalFiles[0].getName();

        for (int i = 0; i < 100; i++) {
            store.put("key" + i, "a_long_value_to_make_sure_we_cross_the_threshold" + i);
        }

        Thread.sleep(2000);

        File[] finalWalFiles = dataDir.listFiles((dir, name) -> name.endsWith(WriteAheadLog.WAL_FILE_EXTENSION));
        assertNotNull(finalWalFiles);
        assertEquals(1, finalWalFiles.length, "Should still have only one active WAL segment");
        String finalWalName = finalWalFiles[0].getName();

        assertNotEquals(initialWalName, finalWalName, "The active WAL segment should be a new file");

        File oldWalFile = new File(config.getDataDirectory(), initialWalName);
        assertFalse(oldWalFile.exists(), "The retired WAL segment should have been deleted");
    }

    @Test
    void testDelete() {
        store.put("key1", "value1");
        assertEquals("value1", store.get("key1").orElse(null));

        store.delete("key1");
        assertFalse(store.get("key1").isPresent(), "Key should be deleted");
    }

    @Test
    void testDeleteIsDurable() throws IOException {
        store.put("key1", "value1");
        store.delete("key1");
        store.stop();

        KeyValueStore newStore = new DurableStore(config);
        newStore.start();
        assertFalse(newStore.get("key1").isPresent(), "Deletion should persist after restart");
        newStore.stop();
        
        this.store = null;
    }

    
    @Test
    void testCompaction() throws InterruptedException, IOException {
        for (int i = 0; i < config.getCompactionTriggerFileCount(); i++) {
            ConcurrentSkipListMap<String, String> tempMemtable = new ConcurrentSkipListMap<>();
            tempMemtable.put("key" + i, "value" + i);
            Path sstPath = store.sstableWriter.write(tempMemtable);
            store.sstableManager.addNewSSTable(sstPath);
        }

        File dataDir = new File(config.getDataDirectory());
        assertEquals(config.getCompactionTriggerFileCount(), dataDir.listFiles((dir, name) -> name.endsWith(".sst")).length);

        Thread.sleep(1000);

        File[] sstFiles = dataDir.listFiles((dir, name) -> name.endsWith(".sst"));
        assertNotNull(sstFiles);
        assertEquals(1, sstFiles.length, "All SSTables should have been merged into one");

        assertTrue(store.get("key0").isPresent());
        assertEquals("value0", store.get("key0").get());
        assertTrue(store.get("key3").isPresent());
        assertEquals("value3", store.get("key3").get());
    }
}