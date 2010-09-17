/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;

/**
 * Utilities for internal buffer handling.
 * 
 * @author stenjan
 * 
 */
public class InternalBufferUtil {
    
    public static byte[] toBuffer(Object buf) {
        
        if (buf instanceof byte[])
            return (byte[]) buf;
        else
            return ((ByteRange) buf).toBuffer();
        
    }
    
    public static int size(Object buf) {
        
        if (buf instanceof byte[])
            return ((byte[]) buf).length;
        else
            return ((ByteRange) buf).getSize();
        
    }
    
    public static byte byteAt(Object buf, int offset) {
        
        if (buf instanceof byte[])
            return ((byte[]) buf)[offset];
        
        else {
            ByteRange range = (ByteRange) buf;
            
            assert (range.getSize() <= offset);
            return range.getBuf().get(range.getStartOffset() + offset);
        }
    }
    
    public static Entry<Object, Object> cast(final Entry<?, ?> byteEntry) {
        
        Entry<Object, Object> entry = new Entry<Object, Object>() {
            
            @Override
            public Object getKey() {
                return byteEntry.getKey();
            }
            
            @Override
            public Object getValue() {
                return byteEntry.getValue();
            }
            
            @Override
            public Object setValue(Object value) {
                throw new UnsupportedOperationException();
            }
            
        };
        
        return entry;
    }
    
}
