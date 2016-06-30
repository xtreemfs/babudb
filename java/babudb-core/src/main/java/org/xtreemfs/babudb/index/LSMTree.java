/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.overlay.MultiOverlayBufferTree;
import org.xtreemfs.babudb.index.reader.DiskIndex;
import org.xtreemfs.babudb.index.reader.InternalBufferUtil;
import org.xtreemfs.babudb.index.reader.InternalDiskIndexIterator;
import org.xtreemfs.babudb.index.reader.InternalMergeIterator;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.OutputUtils;

public class LSMTree {
    
    private static long               totalOnDiskSize = 0;
    
    private static final byte[]       NULL_ELEMENT    = new byte[0];
    
    private MultiOverlayBufferTree    overlay;
    
    private DiskIndex                 index;
    
    private final ByteRangeComparator comp;
    
    private final Object              lock;
    
    private boolean                   compressed;
    
    private final int                 maxEntriesPerBlock;
    
    private final int                 maxBlockFileSize;
    
    private final boolean             useMMap;
    
    private final int                 mmapLimitBytes;
    
    /**
     * Creates a new LSM tree.
     * 
     * @param indexFile
     *            the on-disk index file - may be <code>null</code>
     * @param comp
     *            a comparator for byte ranges
     * @param compressed
     *            Compression of disk-index
     * @throws IOException
     *             if an I/O error occurs when accessing the on-disk index file
     */
    public LSMTree(String indexFile, ByteRangeComparator comp, boolean compressed, int maxEntriesPerBlock,
        int maxBlockFileSize, boolean useMMap, int mmapLimit) throws IOException {
        
        this.comp = comp;
        this.compressed = compressed;
        this.maxEntriesPerBlock = maxEntriesPerBlock;
        this.maxBlockFileSize = maxBlockFileSize;
        this.useMMap = useMMap;
        this.mmapLimitBytes = mmapLimit * 1024 * 1024;
        
        overlay = new MultiOverlayBufferTree(NULL_ELEMENT, comp);
        totalOnDiskSize += indexFile == null ? 0 : getTotalDirSize(new File(indexFile));
        index = indexFile == null ? null : new DiskIndex(indexFile, comp, compressed, useMmap());
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
        
        if (result == NULL_ELEMENT)
            return null;
        
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
        
        if (result == NULL_ELEMENT)
            return null;
        
        if (result != null)
            return result;
        
        return index == null ? null : index.lookup(key);
    }
    
    /**
     * Returns the first entry.
     * 
     * @return the first entry
     */
    public Entry<byte[], byte[]> firstEntry() {
        ResultSet<byte[], byte[]> it = prefixLookup(new byte[0]);
        Entry<byte[], byte[]> result = it.hasNext() ? it.next() : null;
        it.free();
        return result;
    }
    
    /**
     * Returns the first entry in the given snapshot.
     * 
     * @param snapId
     *            the snapshot ID
     * @return the first entry
     */
    public Entry<byte[], byte[]> firstEntry(int snapId) {
        ResultSet<byte[], byte[]> it = prefixLookup(new byte[0], snapId, true);
        Entry<byte[], byte[]> result = it.hasNext() ? it.next() : null;
        it.free();
        return result;
    }
    
    /**
     * Returns the last entry.
     * 
     * @return the last entry
     */
    public Entry<byte[], byte[]> lastEntry() {
        ResultSet<byte[], byte[]> it = prefixLookup(new byte[0], false);
        Entry<byte[], byte[]> result = it.hasNext() ? it.next() : null;
        it.free();
        return result;
    }
    
    /**
     * Returns the last entry in the given snapshot.
     * 
     * @param snapId
     *            the snapshot ID
     * @return the last entry
     */
    public Entry<byte[], byte[]> lastEntry(int snapId) {
        ResultSet<byte[], byte[]> it = prefixLookup(new byte[0], snapId, false);
        Entry<byte[], byte[]> result = it.hasNext() ? it.next() : null;
        it.free();
        return result;
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
    public ResultSet<byte[], byte[]> prefixLookup(byte[] prefix) {
        return prefixLookup(prefix, true);
    }
    
    /**
     * Performs a prefix lookup. Key-value paris are returned in an iterator in
     * the given key order, where only such keys are returned with a matching
     * prefix according to the comparator.
     * 
     * @param prefix
     *            the prefix
     * @param ascending
     *            if <code>true</code>, entries will be returned in ascending
     *            order; otherwise, they will be returned in descending order
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> prefixLookup(byte[] prefix, boolean ascending) {
        
        if (prefix != null && prefix.length == 0)
            prefix = null;
        
        List<Iterator<Entry<byte[], byte[]>>> list = new ArrayList<Iterator<Entry<byte[], byte[]>>>(2);
        list.add(overlay.prefixLookup(prefix, true, ascending));
        if (index != null) {
            byte[][] rng = comp.prefixToRange(prefix, ascending);
            list.add(index.rangeLookup(rng[0], rng[1], ascending));
        }
        
        return new OverlayMergeIterator<byte[], byte[]>(list, comp, NULL_ELEMENT, ascending);
    }
    
    /**
     * Performs a prefix lookup in a given snapshot. Key-value paris are
     * returned in an iterator in ascending key order, where only such keys are
     * returned with a matching prefix according to the comparator.
     * 
     * @param prefix
     *            the prefix
     * @param snapId
     *            the snapshot ID
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> prefixLookup(byte[] prefix, int snapId) {
        return prefixLookup(prefix, snapId, true);
    }
    
    /**
     * Performs a prefix lookup in a given snapshot. Key-value pairs are
     * returned in an iterator in the given key order, where only such keys are
     * returned with a matching prefix according to the comparator.
     * 
     * @param prefix
     *            the prefix
     * @param snapId
     *            the snapshot ID
     * @param ascending
     *            if <code>true</code>, entries will be returned in ascending
     *            order; otherwise, they will be returned in descending order
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> prefixLookup(byte[] prefix, int snapId, boolean ascending) {
        
        if (prefix != null && prefix.length == 0)
            prefix = null;
        
        List<Iterator<Entry<byte[], byte[]>>> list = new ArrayList<Iterator<Entry<byte[], byte[]>>>(2);
        list.add(overlay.prefixLookup(prefix, snapId, true, ascending));
        if (index != null) {
            byte[][] rng = comp.prefixToRange(prefix, ascending);
            list.add(index.rangeLookup(rng[0], rng[1], ascending));
        }
        
        return new OverlayMergeIterator<byte[], byte[]>(list, comp, NULL_ELEMENT, ascending);
    }
    
    /**
     * Performs a range lookup. Key-value paris are returned in an iterator in
     * ascending key order, where only such keys are returned between
     * <code>from</code> (inclusively) and <code>to</code> (inclusively),
     * according to the comparator.
     * 
     * @param from
     *            the first key (inclusively)
     * @param to
     *            the last key (exclusively)
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> rangeLookup(byte[] from, byte[] to) {
        return rangeLookup(from, to, true);
    }
    
    /**
     * Performs a range lookup. Key-value paris are returned in an iterator in
     * the given key order, where only such keys are returned between
     * <code>from</code> (inclusively) and <code>to</code> (inclusively),
     * according to the comparator.
     * 
     * @param from
     *            the first key (inclusively)
     * @param to
     *            the last key (exclusively)
     * @param ascending
     *            if <code>true</code>, entries will be returned in ascending
     *            order; otherwise, they will be returned in descending order
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> rangeLookup(byte[] from, byte[] to, boolean ascending) {
        
        if (from.length == 0)
            from = null;
        
        if (to.length == 0)
            to = null;
        
        List<Iterator<Entry<byte[], byte[]>>> list = new ArrayList<Iterator<Entry<byte[], byte[]>>>(2);
        list.add(overlay.rangeLookup(from, to, true, ascending));
        if (index != null)
            list.add(index.rangeLookup(from, to, ascending));
        
        return new OverlayMergeIterator<byte[], byte[]>(list, comp, NULL_ELEMENT, ascending);
    }
    
    /**
     * Performs a range lookup in a given snapshot. Key-value paris are returned
     * in an iterator in ascending key order, where only such keys are returned
     * between <code>from</code> (inclusively) and <code>to</code>
     * (inclusively), according to the comparator.
     * 
     * @param from
     *            the first key (inclusively)
     * @param to
     *            the last key (exclusively)
     * @param snapId
     *            the snapshot ID
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> rangeLookup(byte[] from, byte[] to, int snapId) {
        return rangeLookup(from, to, snapId, true);
    }
    
    /**
     * Performs a range lookup in a given snapshot. Key-value pairs are returned
     * in an iterator in the given key order, where only such keys are returned
     * between <code>from</code> (inclusively) and <code>to</code>
     * (inclusively), according to the comparator.
     * 
     * @param from
     *            the first key (inclusively)
     * @param to
     *            the last key (exclusively)
     * @param snapId
     *            the snapshot ID
     * @param ascending
     *            if <code>true</code>, entries will be returned in ascending
     *            order; otherwise, they will be returned in descending order
     * @return an iterator with key-value pairs
     */
    public ResultSet<byte[], byte[]> rangeLookup(byte[] from, byte[] to, int snapId, boolean ascending) {
        
        if (from.length == 0)
            from = null;
        
        if (to.length == 0)
            to = null;
        
        List<Iterator<Entry<byte[], byte[]>>> list = new ArrayList<Iterator<Entry<byte[], byte[]>>>(2);
        list.add(overlay.rangeLookup(from, to, snapId, true, ascending));
        if (index != null)
            list.add(index.rangeLookup(from, to, ascending));
        
        return new OverlayMergeIterator<byte[], byte[]>(list, comp, NULL_ELEMENT, ascending);
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
        synchronized (lock) {
            overlay.insert(key, null);
        }
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
        
        DiskIndexWriter writer = new DiskIndexWriter(targetFile, maxEntriesPerBlock, compressed,
            maxBlockFileSize);
        
        InternalMergeIterator it = internalPrefixLookup(null, snapId, true);
        writer.writeIndex(it);
        it.free();
    }
    
    /**
     * Writes a certain part of an in-memory snapshot to a file on disk.
     * 
     * @param targetFile
     *            the file to which to write the snapshot
     * @param snapId
     *            the snapshot ID
     * @param indexId
     *            the id used by the database to identify this index
     * @param snap
     *            the snapshot configuration
     * @throws IOException
     *             if an I/O error occurs while writing the snapshot
     */
    public void materializeSnapshot(String targetFile, final int snapId, final int indexId,
        final SnapshotConfig snap) throws IOException {
        
        DiskIndexWriter writer = new DiskIndexWriter(targetFile, maxEntriesPerBlock, compressed,
            maxBlockFileSize);
        writer.writeIndex(new ResultSet<Object, Object>() {
            
            private ResultSet<Object, Object>[] iterators;
            
            private Entry<Object, Object>             next;
            
            private int                               currentIt;
            
            {
                byte[][] prefixes = snap.getPrefixes(indexId);
                
                currentIt = 0;
                
                if (prefixes != null) {
                    iterators = new ResultSet[prefixes.length];
                    for (int i = 0; i < prefixes.length; i++)
                        iterators[i] = internalPrefixLookup(prefixes[i], snapId, true);
                } else {
                    iterators = new ResultSet[] { prefixLookup(null, snapId, true) };
                }
                
                getNextElement();
                
            }
            
            @Override
            public boolean hasNext() {
                return (next != null);
            }
            
            @Override
            public Entry<Object, Object> next() {
                
                if (next == null)
                    throw new NoSuchElementException();
                
                Entry<Object, Object> tmp = next;
                getNextElement();
                
                return tmp;
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public void free() {
                for (ResultSet<Object, Object> it : iterators)
                    it.free();
            }
            
            private void getNextElement() {
                
                for (;;) {
                    
                    // get the next iterator w/ elements
                    while (currentIt < iterators.length && !iterators[currentIt].hasNext())
                        currentIt++;
                    
                    // if there is no such iterator, set next to null and return
                    if (currentIt >= iterators.length) {
                        next = null;
                        return;
                    }
                    
                    // otherwise, next is the next element from the current
                    // iterator
                    next = iterators[currentIt].next();
                    
                    // if this element is explicitly excluded, skip it
                    byte[] tmp = InternalBufferUtil.toBuffer(next.getKey());
                    if (snap.containsKey(indexId, tmp))
                        break;
                }
            }
            
        });
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
            totalOnDiskSize -= index == null ? 0 : index.getSize();
            index = new DiskIndex(snapshotFile, comp, this.compressed, useMmap());
            totalOnDiskSize += index.getSize();
            if (oldIndex != null)
                oldIndex.destroy();
            overlay.cleanup();
        }
    }
    
    /**
     * Checks if files containing index data are memory-mapped.
     * 
     * @return <code>true</code> if index files are memory-mapped, <code>false</code>, otherwise
     */
    public boolean isMMapEnabled() {
    	return useMMap;
    }
    
    /**
     * Checks if files containing index data are compressed.
     * 
     * @return <code>true</code> if index files are compressed, <code>false</code>, otherwise
     */
    public boolean isCompressed() {
    	return compressed;
    }
    
    /**
     * Destroys the LSM tree. Frees all in-memory indices plus the on-disk tree.
     */
    public void destroy() throws IOException {
        
        synchronized (lock) {
            if (index != null) {
                totalOnDiskSize -= index.getSize();
                index.destroy();
            }
            overlay.cleanup();
        }
    }
    
    private boolean useMmap() {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "DB size: " + OutputUtils.formatBytes(totalOnDiskSize));
        return useMMap && (mmapLimitBytes < 0 || totalOnDiskSize < mmapLimitBytes);
    }
    
    /**
     * Performs a prefix lookup. Key-value paris are returned in an iterator in
     * the given key order, where only such keys are returned with a matching
     * prefix according to the comparator. <br/>
     * 
     * <b>WARNING:</b> This method should only be accessed internally, as it
     * provides access to internal index buffers that have to remain immutable.
     * 
     * @param prefix
     *            the prefix
     * @param snapId
     *            the snapshot ID
     * @param ascending
     *            if <code>true</code>, entries will be returned in ascending
     *            order; otherwise, they will be returned in descending order
     * @return an iterator with references to internally used buffers
     */
    protected InternalMergeIterator internalPrefixLookup(byte[] prefix, int snapId, boolean ascending) {
        
        if (prefix != null && prefix.length == 0)
            prefix = null;
        
        Iterator<Entry<byte[], byte[]>> overlayIterator = overlay.prefixLookup(prefix, snapId, true,
            ascending);
        InternalDiskIndexIterator diskIndexIterator = null;
        if (index != null) {
            byte[][] rng = comp.prefixToRange(prefix, ascending);
            diskIndexIterator = index.internalRangeLookup(rng[0], rng[1], ascending);
        }
        
        return new InternalMergeIterator(overlayIterator, diskIndexIterator, comp, NULL_ELEMENT, ascending);
    }
    
    private static long getTotalDirSize(File dir) {
        
        if (!dir.exists())
            return 0;
        
        long size = 0;
        if (dir.isDirectory()) {
            for (File child : dir.listFiles())
                size += getTotalDirSize(child);
        } else
            size += dir.length();
        
        return size;
    }
    
}
