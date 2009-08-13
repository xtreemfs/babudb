/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

/**
 * A set of configuration parameters for a new snapshot.
 * 
 * @author stender
 * 
 */
public interface SnapshotConfig {
    
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
     * Returns an array of key prefixes for a given index. All records to be
     * included in the snapshot of this index have to match (exactly) one of the
     * prefixes.
     * 
     * NOTE: Implementations of this interface have to ensure that no two
     * prefixes may cover one another.
     * 
     * @param index
     *            the index
     * @return the array of key prefixes
     */
    public byte[][] getPrefixes(int index);
    
}
