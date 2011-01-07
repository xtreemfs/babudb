/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api.index;

import java.util.Comparator;

import org.xtreemfs.babudb.index.ByteRange;

/**
 * A comparator for byte buffers and byte ranges.
 * 
 * @author stender
 * 
 */
public interface ByteRangeComparator extends Comparator<byte[]> {
    
    /**
     * Compares a range of bytes from a potentially large buffer to the entire
     * content of a given buffer.<br>
     * 
     * This method should be implemented efficiently, as it may be invoked a
     * large number of times withe each database lookup.
     * 
     * @param rng
     *            the range
     * @param buf
     *            the buffer
     * @return a negative value if <code>buf</code> is considered as smaller
     *         than <code>rng</code>, 0 if both are considered as equal, and a
     *         positive value if <code>buf</code> is considered as greater.
     */
    public int compare(ByteRange rng, byte[] buf);
    
    /**
     * Converts a prefix to a range. The method is needed to translate prefix
     * queries into range queries.
     * 
     * @param prefix
     *            a buffer representing the prefix to query
     * @param ascending
     *            if <code>true</code>, the lower bound is the first value in
     *            the range; otherwise, it is the second value
     * @return an array consisting of two buffers, the first being the inclusive
     *         lower bound of the range, the second being the exclusive upper
     *         bound of the range
     */
    public byte[][] prefixToRange(byte[] prefix, boolean ascending);
    
}
