/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.index.reader;

import java.nio.ByteBuffer;

import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Base class for block readers.
 * 
 * @author stenjan
 * 
 */
public abstract class BlockReader {
    
    protected ByteBuffer          buffer;    // for buffered block readers
                                              
    protected ReusableBuffer      readBuffer; // for streamed block readers
                                              
    protected int                 position;
    
    protected int                 limit;
    
    protected ByteRangeComparator comp;
    
    protected MiniPage            keys;
    
    protected MiniPage            values;
    
    protected int                 numEntries;
    
    protected final boolean       isBuffered;
    
    protected BlockReader(boolean isBuffered) {
        this.isBuffered = isBuffered;
    }
    
    public BlockReader clone() {
        
        assert (isBuffered);
        
        buffer.position(0);
        return new DefaultBlockReader(buffer.slice(), position, limit, comp);
    }
    
    public abstract ByteRange lookup(byte[] key);
    
    public abstract ResultSet<ByteRange, ByteRange> rangeLookup(byte[] from, byte[] to,
        final boolean ascending);
    
    public MiniPage getKeys() {
        return keys;
    }
    
    public MiniPage getValues() {
        return values;
    }
    
    public int getNumEntries() {
        return numEntries;
    }
    
    public void free() {
        if (readBuffer != null)
            BufferPool.free(readBuffer);
    }
    
}