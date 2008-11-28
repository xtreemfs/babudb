/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.index.writer;

import java.util.LinkedList;
import java.util.List;

import org.xtreemfs.babudb.index.reader.BlockReader;
import org.xtreemfs.common.buffer.BufferPool;
import org.xtreemfs.common.buffer.ReusableBuffer;

public class BlockWriter {
    
    private List<byte[]> keys;
    
    private List<byte[]> values;
    
    private boolean      varLenKeys;
    
    private boolean      varLenVals;
    
    public BlockWriter(boolean varLenKeys, boolean varLenVals) {
        
        keys = new LinkedList<byte[]>();
        values = new LinkedList<byte[]>();
        
        this.varLenKeys = varLenKeys;
        this.varLenVals = varLenVals;
    }
    
    public void add(byte[] key, byte[] value) {
        keys.add(key);
        values.add(value);
    }
    
    public ReusableBuffer serialize() {
        
        ReusableBuffer keyBuf = varLenKeys ? serializeVarLenPage(keys)
            : serializeFixedLenPage(keys);
        ReusableBuffer valBuf = varLenVals ? serializeVarLenPage(values)
            : serializeFixedLenPage(values);
        
        int entries = keys.size();
        int valsOffset = BlockReader.KEYS_OFFSET + keyBuf.limit();
        
        ReusableBuffer returnBuf = BufferPool.allocate(valsOffset + valBuf.limit());
        returnBuf.putInt(valsOffset);
        returnBuf.putInt(entries);
        returnBuf.putInt(varLenKeys ? -1 : entries == 0 ? 0 : (keyBuf.limit() / entries));
        returnBuf.putInt(varLenVals ? -1 : entries == 0 ? 0 : (valBuf.limit() / entries));
        returnBuf.put(keyBuf);
        returnBuf.put(valBuf);
        
        BufferPool.free(keyBuf);
        BufferPool.free(valBuf);
        
        returnBuf.position(0);
        
        return returnBuf;
    }
    
    public byte[] getBlockKey() {
        return keys.get(0);
    }
    
    private static ReusableBuffer serializeVarLenPage(List<byte[]> list) {
        
        List<Integer> offsets = new LinkedList<Integer>();
        int size = 0;
        for (byte[] buf : list) {
            size += buf.length;
            offsets.add(size);
        }
        
        size += offsets.size() * Integer.SIZE / 8;
        
        ReusableBuffer newBuf = BufferPool.allocate(size);
        for (byte[] buf : list)
            newBuf.put(buf);
        
        for (int offs : offsets)
            newBuf.putInt(offs);
        
        newBuf.position(0);
        
        return newBuf;
    }
    
    private static ReusableBuffer serializeFixedLenPage(List<byte[]> list) {
        
        final int size = list.size() == 0 ? 0 : list.get(0).length * list.size();
        
        ReusableBuffer newBuf = BufferPool.allocate(size);
        for (byte[] buf : list)
            newBuf.put(buf);
        
        newBuf.position(0);
        
        return newBuf;
    }
    
}
