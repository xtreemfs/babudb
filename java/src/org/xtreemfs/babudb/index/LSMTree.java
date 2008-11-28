/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.overlay.MultiOverlayBufferTree;
import org.xtreemfs.babudb.index.reader.DiskIndex;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;

public class LSMTree {
    
    private static final int          MAX_ENTRIES_PER_BLOCK = 16;
    
    private static final byte[]       NULL_ELEMENT          = new byte[0];
    
    private MultiOverlayBufferTree    overlay;
    
    private DiskIndex                 index;
    
    private final ByteRangeComparator comp;
    
    private final Object              lock;
    
    public LSMTree(String indexFile, ByteRangeComparator comp) throws IOException {
        
        this.comp = comp;
        
        overlay = new MultiOverlayBufferTree(NULL_ELEMENT, comp);
        index = indexFile == null ? null : new DiskIndex(indexFile, comp);
        lock = new Object();
    }
    
    /**
     * Performs a lookup.
     * 
     * @param key
     *            the key to look up
     * @return the value associated with the key
     */
    public byte[] lookup(byte[] key) {
        
        byte[] result = overlay.lookup(key);
        if (result != null)
            return result;
        
        return index == null ? null : index.lookup(key);
    }
    
    /**
     * Performs a lookup in a given snapshot.
     * 
     * @param key
     *            the key to look up
     * @param snapId
     *            the snapshot ID
     * @return the value associated with the key in the snapshot
     */
    public byte[] lookup(byte[] key, int snapId) {
        
        byte[] result = overlay.lookup(key, snapId);
        if (result != null)
            return result;
        
        return index == null ? null : index.lookup(key);
    }
    
    /**
     * Performs a prefix lookup. Key-value paris are returned in an iterator in
     * ascending key order, where only such keys are returned with a matching
     * prefix according to the comparator.
     * 
     * @param prefix
     *            the prefix
     * @return an iterator with key-value pairs
     */
    public Iterator<Entry<byte[], byte[]>> prefixLookup(byte[] prefix) {
        
        if (prefix.length == 0)
            prefix = null;
        
        List<Iterator<Entry<byte[], byte[]>>> list = new ArrayList<Iterator<Entry<byte[], byte[]>>>(
            2);
        list.add(overlay.prefixLookup(prefix));
        if (index != null) {
            byte[][] rng = comp.prefixToRange(prefix);
            list.add(index.rangeLookup(rng[0], rng[1]));
        }
        
        return new OverlayMergeIterator<byte[], byte[]>(list, comp, null);
    }
    
    /**
     * Performs a prefix lookup in a given snapshot. Key-value paris are
     * returned in an iterator in ascending key order, where only such keys are
     * returned with a matching prefix according to the comparator.
     * 
     * @param prefix
     *            the prefix
     * @return an iterator with key-value pairs
     */
    public Iterator<Entry<byte[], byte[]>> prefixLookup(byte[] prefix, int snapId) {
        
        if (prefix != null && prefix.length == 0)
            prefix = null;
        
        List<Iterator<Entry<byte[], byte[]>>> list = new ArrayList<Iterator<Entry<byte[], byte[]>>>(
            2);
        list.add(overlay.prefixLookup(prefix, snapId));
        if (index != null) {
            byte[][] rng = comp.prefixToRange(prefix);
            list.add(index.rangeLookup(rng[0], rng[1]));
        }
        
        return new OverlayMergeIterator<byte[], byte[]>(list, comp, null);
    }
    
    /**
     * Inserts a key-value pair.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void insert(byte[] key, byte[] value) {
        synchronized (lock) {
            overlay.insert(key, value);
        }
    }
    
    /**
     * Deletes a key-value pair. This method is equivalent to
     * <code>insert(key, null)</code>.
     * 
     * @param key
     *            the key
     */
    public void delete(byte[] key) {
        overlay.insert(key, null);
    }
    
    /**
     * Creates a new in-memory snapshot.
     * 
     * @return the snapshot ID
     */
    public int createSnapshot() {
        return overlay.newOverlay();
    }
    
    /**
     * Writes an in-memory snapshot to a file on disk.
     * 
     * @param targetFile
     *            the file to which to write the snapshot
     * @param snapId
     *            the snapshot ID
     * @throws IOException
     *             if an I/O error occurs while writing the snapshot
     */
    public void materializeSnapshot(String targetFile, int snapId) throws IOException {
        DiskIndexWriter writer = new DiskIndexWriter(targetFile, MAX_ENTRIES_PER_BLOCK);
        writer.writeIndex(prefixLookup(null, snapId));
    }
    
    /**
     * Links the LSM tree to a new snapshot file. The on-disk index is replaced
     * with the index stored in the given snapshot file, and all in-memory
     * snapshots are discarded.
     * 
     * @param snapshotFile
     *            the snapshot file
     * @throws IOException
     *             if an I/O error occurred while reading the snapshot file
     */
    public void linkToSnapshot(String snapshotFile) throws IOException {
        final DiskIndex oldIndex = index;
        synchronized (lock) {
            index = new DiskIndex(snapshotFile, comp);
            if (oldIndex != null)
                oldIndex.destroy();
            overlay.cleanup();
        }
    }
    
}
