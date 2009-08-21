/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.nio.ByteBuffer;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.babudb.index.ByteRangeComparator;

public abstract class MiniPage {
    
    protected int                       numEntries;
    
    protected ByteBuffer                buf;
    
    protected final int                 offset;
    
    protected final ByteRangeComparator comp;
    
    public MiniPage(int numEntries, ByteBuffer buf, int offset, ByteRangeComparator comp) {
        
        // ensure that offs > limit
        assert (offset <= buf.limit()) : "invalid mini page offset: offset == " + offset
            + ", buf.limit() == " + buf.limit();
        
        // if offs == limit, numEntries must be 0 (empty page)
        assert (offset != buf.limit() || numEntries == 0) : "invalid mini page offset: offset == " + offset
            + ", buf.limit() == " + buf.limit();
        
        this.numEntries = numEntries;
        this.buf = buf;
        this.offset = offset;
        this.comp = comp;
    }
    
    /**
     * Returns the number of entries in the page.
     * 
     * @return the number of entries
     */
    public int getNumEntries() {
        return numEntries;
    }
    
    /**
     * Returns an entry at a given index position.
     * 
     * @param n
     *            the index position
     * @return a byte range representing the entry
     */
    public abstract ByteRange getEntry(int n);
    
    /**
     * Returns the position of an entry in the page. If the entry is not
     * contained, -1 is returned.
     * 
     * @param entry
     *            the entry to look up
     * @return the position of the entry, or -1, if the entry is not contained
     */
    public int getPosition(byte[] entry) {
        return SearchUtil.getOffset(this, entry, comp);
    }
    
    /**
     * Returns the position of an entry in the page. Unlike
     * <code>getPosition()</code>, it returns the position of the next larger
     * entry if the entry is not contained. If the entry to search for is larger
     * than the largest entry in the page, the last index position incremented
     * by 1 is returned.
     * 
     * @param entry
     *            the entry to look up
     * @return the position of the entry, or the last index position + 1, if the
     *         entry is beyond the range of entries
     */
    public int getTopPosition(byte[] entry) {
        
        if (entry == null)
            return 0;
        
        return SearchUtil.getInclTopOffset(this, entry, comp);
    }
    
    /**
     * Returns the position of the entry that is next smaller compared to the
     * given entry. If the next smaller entry is smaller than the first entry in
     * the page, -1 is returned. If the given entry is null, the last index
     * position is returned.
     * 
     * @param entry
     *            the entry for which to find the next smaller entry
     * @return the position of the next smaller entry, or -1, if the next
     *         smaller entry is smaller than the first entry
     */
    public int getExclBottomPosition(byte[] entry) {
        
        if (entry == null)
            return numEntries - 1;
        
        return SearchUtil.getExclBottomOffset(this, entry, comp);
    }
    
    /**
     * Returns the position of the entry that is next smaller or equal entry
     * compared to the given entry. If the next smaller entry is smaller than
     * the first entry in the page, -1 is returned. If the given entry is null,
     * the last index position is returned.
     * 
     * @param entry
     *            the entry for which to find the next smaller entry
     * @return the position of the next smaller entry, or -1, if the next
     *         smaller entry is smaller than the first entry
     */
    public int getInclBottomPosition(byte[] entry) {
        
        if (entry == null)
            return numEntries - 1;
        
        return SearchUtil.getInclBottomOffset(this, entry, comp);
    }
    
}
