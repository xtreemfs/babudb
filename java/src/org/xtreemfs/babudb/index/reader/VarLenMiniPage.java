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
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.util.OutputUtils;

public class VarLenMiniPage extends MiniPage {
    
    private final int offsetListStart;
    
    public VarLenMiniPage(int numEntries, ByteBuffer buf, int offset, int limit,
        ByteRangeComparator comp) {
        
        super(numEntries, buf, offset, comp);
        
        // calculate the offset of the offset list
        offsetListStart = limit - numEntries * Integer.SIZE / 8;
    }
    
    public ByteRange getEntry(int n) {
        
        int offsetStart = offset;
        if (n > 0)
            offsetStart += buf.getInt(offsetListStart + (n - 1) * Integer.SIZE / 8);
        
        int offsetEnd = offset;
        offsetEnd += buf.getInt(offsetListStart + n * Integer.SIZE / 8);
        
        assert (offsetEnd > offsetStart);
        
        return new ByteRange(buf, offsetStart, offsetEnd);
    }
    
    public String toString() {
        
        buf.position(offset);
        buf.limit(offsetListStart + numEntries * Integer.SIZE / 8);
        ReusableBuffer newBuf = BufferPool.allocate(buf.limit() - buf.position());
        newBuf.put(buf);
        String result = OutputUtils.byteArrayToFormattedHexString(newBuf.array());
        BufferPool.free(newBuf);
        buf.clear();
        
        return result;
    }
}
