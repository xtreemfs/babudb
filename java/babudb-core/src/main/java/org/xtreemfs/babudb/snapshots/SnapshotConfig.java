/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.io.Serializable;

/**
 * A set of configuration parameters for a new snapshot.
 * 
 * @author stender
 * 
 */
public interface SnapshotConfig extends Serializable {
    
    /**
     * Returns the name for the new snapshot.
     * 
     * @return the name
     */
    public String getName();
    
    /**
     * Returns an array of indices to be included in the new snapshot.
     * 
     * @return a set of indices
     */
    public int[] getIndices();
    
    /**
     * Returns an array of prefix keys that are supposed to be written to the
     * snapshot of the given index. This method is used to pre-select certain
     * key ranges, so that only parts of each index may have to be traversed
     * when writing the snapshot to disk.
     * 
     * Implementations have to make sure that the array of prefix keys returned
     * for a given index map is sorted in ascending order, and that prefixes
     * from the array do not cover one another.
     * 
     * @param index
     *            the index
     * @return An array of byte arrays, where each byte array represents a key
     *         prefix for the given index. If the prefix key is
     *         <code>null</code>, the whole index will be traversed.
     */
    public byte[][] getPrefixes(int index);
    
    /**
     * Checks if the given key in the given index is contained in the snapshot.
     * Note that this check will only be performed for keys that are covered by
     * (one of) the prefix keys returned by <code>getPrefix(index)</code>.
     * 
     * @param index
     *            the index
     * @param key
     *            the key
     * @return <code>true</code>, if the key is part of the snapshot,
     *         <code>false</code>, otherwise
     */
    public boolean containsKey(int index, byte[] key);
    
}
