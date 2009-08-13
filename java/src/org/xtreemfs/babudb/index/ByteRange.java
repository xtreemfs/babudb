/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index;

import java.nio.ByteBuffer;

import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.util.OutputUtils;

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
    
    private byte[]	   prefix;
    
    public ByteRange(ByteBuffer buf, int startOffset, int endOffset) {
        
        this.buf = buf;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.size = endOffset - startOffset;
        this.prefix = null;
        
        assert (endOffset < buf.limit()) : "buf.limit() == " + buf.limit() + ", endOffset == "
            + endOffset + ", startOffset == " + startOffset + ", buf.capacity == " + buf.capacity();
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
    
    public void addPrefix(byte[] prefix) {
    	this.prefix = prefix;
    }
    
    public byte[] toBuffer() {
    	byte[] tmp;

        buf.position(startOffset);

    	if(prefix == null) {
    		tmp = new byte[size];
    		buf.get(tmp);
    	} else {
    		tmp = new byte[prefix.length + size];
    		System.arraycopy(prefix, 0, tmp, 0, prefix.length);
            buf.get(tmp, prefix.length, size);
    	}
    	
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
