package com.ganesh.db.wal;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.google.common.hash.Funnel;
/**
 * Represents an immutable entry for a single key-value operation in the database.
 * <p>
 * This class is used for entries in both the Write-Ahead Log (WAL) and the SSTables.
 * It supports logical deletions through a special "tombstone" marker. An entry with a
 * tombstone value signifies that the corresponding key has been deleted.
 */
public class LogEntry {
    private final String key;
    private final String value;
    /**
     * A special marker object used to signify the deletion of a key.
     * Using a distinct object reference allows for an efficient identity check (==)
     * in {@link #isTombstone()}.
     */
    public static final String TOMBSTONE = new String("");
    /**
     * A Guava {@link Funnel} for keys. This provides a way to feed keys into a
     * Guava Bloom Filter, specifying how to convert the {@link CharSequence} key
     * into a stream of primitive bytes for hashing.
     */
    public static final Funnel<CharSequence> KEY_FUNNEL = (from, into) -> into.putString(from, StandardCharsets.UTF_8);
    /**
     * Constructs a new LogEntry.
     *
     * @param key   The key for the entry.
     * @param value The value for the entry, or {@link #TOMBSTONE} to mark a deletion.
     */
    public LogEntry(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the key of this log entry.
     *
     * @return The key as a {@link String}.
     */
    public String getKey() { return key; }

    /**
     * Returns the value of this log entry.
     *
     * @return The value as a {@link String}, or the {@link #TOMBSTONE} marker if it's a deletion.
     */
    public String getValue() { return value; }

    /**
     * Checks if this log entry represents a deletion (i.e., is a tombstone).
     *
     * @return {@code true} if the entry is a tombstone, {@code false} otherwise.
     */
    public boolean isTombstone() { return value == TOMBSTONE; }

    /**
     * Serializes this log entry to a binary format and writes it to the given output stream.
     * <p>
     * The binary format is:
     * <ul>
     * <li>Key (UTF-8 String)</li>
     * <li>isTombstone (boolean flag)</li>
     * <li>Value (UTF-8 String, only present if not a tombstone)</li>
     * </ul>
     *
     * @param out The {@link DataOutputStream} to write to.
     * @throws IOException if an I/O error occurs during writing.
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(key);
        // BUG FIX: Corrected typo from isTTombstone to isTombstone
        if (isTombstone()) {
            out.writeBoolean(true);
        } else {
            out.writeBoolean(false);
            out.writeUTF(value);
        }
    }

    /**
     * Reads a log entry from a binary stream, deserializing it into a new {@code LogEntry} object.
     * This is a static factory method that complements {@link #writeTo(DataOutputStream)}.
     *
     * @param in The {@link DataInput} stream to read from.
     * @return A new, fully-formed {@link LogEntry} instance.
     * @throws IOException if an I/O error occurs or the stream is malformed.
     */
    public static LogEntry readFrom(DataInput in) throws IOException {
        String key = in.readUTF();
        boolean isTombstone = in.readBoolean();
        if (isTombstone) {
            return new LogEntry(key, TOMBSTONE);
        } else {
            String value = in.readUTF();
            return new LogEntry(key, value);
        }
    }
}