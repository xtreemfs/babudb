/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.writer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.xtreemfs.babudb.index.reader.DefaultBlockReader;
import org.xtreemfs.babudb.index.reader.InternalBufferUtil;

public class DefaultBlockWriter implements BlockWriter {
    
    private List<Object> keys;
    
    private List<Object> values;
    
    private boolean      varLenKeys;
    
    private boolean      varLenVals;
    
    private boolean      serialized;
    
    public DefaultBlockWriter(boolean varLenKeys, boolean varLenVals) {
        
        keys = new LinkedList<Object>();
        values = new LinkedList<Object>();
        
        this.varLenKeys = varLenKeys;
        this.varLenVals = varLenVals;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.index.writer.BlockWriter#add(byte[], byte[])
     */
    public void add(Object key, Object value) {
        
        if (serialized)
            throw new UnsupportedOperationException("already serialized");
        
        keys.add(key);
        values.add(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.index.writer.BlockWriter#serialize()
     */
    public SerializedBlock serialize() {
        
        if (serialized)
            throw new UnsupportedOperationException("already serialized");
        
        serialized = true;
        
        SerializedPage keyPage = varLenKeys ? serializeVarLenPage(keys) : serializeFixedLenPage(keys);
        SerializedPage valPage = varLenVals ? serializeVarLenPage(values) : serializeFixedLenPage(values);
        
        int entries = keys.size();
        int valsOffset = DefaultBlockReader.KEYS_OFFSET + keyPage.size;
        
        // header: [offset of value page, #entries, entry size]
        ByteBuffer tmp = ByteBuffer.wrap(new byte[4 * Integer.SIZE / 8]);
        tmp.putInt(valsOffset);
        tmp.putInt(entries);
        tmp.putInt(varLenKeys ? -1 : entries == 0 ? 0 : (keyPage.size / entries));
        tmp.putInt(varLenVals ? -1 : entries == 0 ? 0 : (valPage.size / entries));
        
        List<Object> header = new ArrayList<Object>(1);
        header.add(tmp.array());
        
        SerializedBlock result = new SerializedBlock();
        result.addBuffers(tmp.limit(), header);
        result.addBuffers(keyPage.size, keyPage.entries);
        result.addBuffers(valPage.size, valPage.entries);
        
        return result;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.index.writer.BlockWriter#getBlockKey()
     */
    public Object getBlockKey() {
        return keys.get(0);
    }
    
    private static SerializedPage serializeVarLenPage(List<Object> list) {
        
        List<Object> offsetList = new LinkedList<Object>();
        
        // append buffers containing all offsets at the end of the list
        int size = 0;
        for (Object buf : list) {
            size += InternalBufferUtil.size(buf);
            final ByteBuffer tmp = ByteBuffer.wrap(new byte[Integer.SIZE / 8]);
            tmp.putInt(size);
            offsetList.add(tmp.array());
        }
        
        size += list.size() * Integer.SIZE / 8;
        
        return new SerializedPage(size, list, offsetList);
    }
    
    private static SerializedPage serializeFixedLenPage(List<Object> list) {
        
        int size = 0;
        for (Object buf : list)
            size += InternalBufferUtil.size(buf);
        
        return new SerializedPage(size, list);
    }
    
}
