/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index;

import java.nio.ByteBuffer;

import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.util.OutputUtils;

/**
 * A range of bytes in a buffer.
 * 
 * @author stender
 */
public class ByteRange {
    
    private ByteBuffer buf;
    
    private int        startOffset;
    
    private int        endOffset;
    
    private int        size;
    
    public ByteRange(ByteBuffer buf, int startOffset, int endOffset) {
        
        this.buf = buf;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.size = endOffset - startOffset;
        
        assert (endOffset >= buf.limit()) : "buffer.limit() == " + buf.limit() + ", endOffset = "
            + endOffset;
    }
    
    public ByteBuffer getBuf() {
        return buf;
    }
    
    public int getStartOffset() {
        return startOffset;
    }
    
    public int getEndOffset() {
        return endOffset;
    }
    
    public int getSize() {
        return size;
    }
    
    public byte[] toBuffer() {
        
        byte[] tmp = new byte[size];
        
        buf.position(startOffset);
        buf.get(tmp);
        return tmp;
        
        // for (int i = startOffset; i < startOffset + size; i++)
        // tmp[i - startOffset] = buf.get(i);
        // return tmp;
    }
    
    public String toString() {
        // return buf.toString() + " [" + startOffset + ", " + endOffset + ")";
        ReusableBuffer buffer = new ReusableBuffer(buf);
        ReusableBuffer newBuf = buffer.createViewBuffer();
        newBuf.range(startOffset, endOffset - startOffset);
        return OutputUtils.byteArrayToHexString(newBuf.array());
    }
    
}
