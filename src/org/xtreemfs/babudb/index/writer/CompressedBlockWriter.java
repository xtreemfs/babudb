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

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.babudb.index.reader.CompressedBlockReader;
import org.xtreemfs.babudb.index.reader.InternalBufferUtil;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

public class CompressedBlockWriter implements BlockWriter {
    
    private List<Object> keys;
    
    private List<Object> values;
    
    private boolean      varLenKeys;
    
    private boolean      varLenVals;
    
    private byte[]       prefix;
    
    public CompressedBlockWriter(boolean varLenKeys, boolean varLenVals) {
        
        keys = new LinkedList<Object>();
        values = new LinkedList<Object>();
        
        this.varLenKeys = varLenKeys;
        this.varLenVals = varLenVals;
    }
    
    public void add(Object key, Object value) {
        keys.add(key);
        values.add(value);
    }
    
    public SerializedBlock serialize() {
        
        // FIXME: implement w/o redundant memory copies
        
        List<byte[]> compressedKeys = compress(keys);
        
        ReusableBuffer keyBuf = varLenKeys ? serializeVarLenPageBuf(compressedKeys)
            : serializeFixedLenPage(keys);
        ReusableBuffer valBuf = varLenVals ? serializeVarLenPage(values) : serializeFixedLenPage(values);
        
        int entries = keys.size();
        int keysOffset = CompressedBlockReader.PREFIX_OFFSET + this.prefix.length;
        int valsOffset = keysOffset + keyBuf.limit();
        
        ByteBuffer returnBuf = ByteBuffer.wrap(new byte[valsOffset + valBuf.limit()]);
        /*
         * the header consist of 4 : ptr to vals 4 : ptr to keys 4 : number of
         * entries 4 : -1 => variable keys, or n => length of fixed size keys 4
         * : -1 => variable values, or n => length of fixed size values k :
         * prefix ... start of keys
         */

        returnBuf.putInt(valsOffset);
        returnBuf.putInt(keysOffset);
        returnBuf.putInt(entries);
        returnBuf.putInt(varLenKeys ? -1 : entries == 0 ? 0 : (keyBuf.limit() / entries));
        returnBuf.putInt(varLenVals ? -1 : entries == 0 ? 0 : (valBuf.limit() / entries));
        
        if (this.prefix.length > 0)
            returnBuf.put(this.prefix);
        
        returnBuf.put(keyBuf.getBuffer());
        returnBuf.put(valBuf.getBuffer());
        
        BufferPool.free(keyBuf);
        BufferPool.free(valBuf);
        
        returnBuf.position(0);
        
        LinkedList<Object> list = new LinkedList<Object>();
        list.add(returnBuf.array());
        SerializedBlock block = new SerializedBlock();
        block.addBuffers(returnBuf.limit(), list);
        
        return block;
    }
    
    public Object getBlockKey() {
        return keys.get(0);
    }
    
    /**
     * Given a list of byte-arrays, compresses the entries and returns a list of
     * compressed entries.
     * 
     * @param list
     * @return Compressed list
     */
    private List<byte[]> compress(List<Object> list) {
        /*
         * the input is assumed to be sorted - find the longest common prefix
         * among the bytes - record the prefix first in the new list - subtract
         * the prefix from each entry and write it out to the list
         */

        List<byte[]> results = new LinkedList<byte[]>();
        
        /*
         * special case when list has only one element this ensures that there
         * will be no prefix
         */
        if (list.size() == 1) {
            
            this.prefix = new byte[0];
            List<byte[]> tmp = new ArrayList<byte[]>(1);
            Object buf = list.get(0);
            tmp.add(InternalBufferUtil.toBuffer(buf));
            
            return tmp;
        }
        
        Object prefix = list.get(0);
        
        /* find the longest common prefix (lcp) */
        // cant be longer than prefix
        int longestPrefixLen = InternalBufferUtil.size(prefix);
        
        for (Object entry : list) {
            int prefixLen = 0;
            int prefixIndex = 0;
            int entryIndex = 0;
            int maxLen = Math.min(InternalBufferUtil.size(prefix), InternalBufferUtil.size(entry));
            
            while (prefixLen < maxLen
                && InternalBufferUtil.byteAt(prefix, prefixIndex++) == InternalBufferUtil.byteAt(entry,
                    entryIndex++)) {
                prefixLen++;
            }
            
            if (prefixLen < longestPrefixLen) {
                longestPrefixLen = prefixLen;
            }
        }
        
        // Create the prefix
        byte[] LCP = new byte[longestPrefixLen];
        System.arraycopy(InternalBufferUtil.toBuffer(prefix), 0, LCP, 0, longestPrefixLen);
        this.prefix = LCP;
        
        // add the entries, removing the prefix
        for (Object entry : list) {
            if (longestPrefixLen <= 0) {
                results.add(InternalBufferUtil.toBuffer(entry));
            } else {
                if (entry instanceof byte[]) {
                    
                    int newLen = ((byte[]) entry).length - longestPrefixLen;
                    byte[] newEntry = new byte[newLen];
                    
                    System.arraycopy((byte[]) entry, longestPrefixLen, newEntry, 0, newLen);
                    results.add(newEntry);
                }

                else {
                    ((ByteRange) entry).addPrefix(LCP);
                    results.add(((ByteRange) entry).toBuffer());
                }
                
            }
        }
        
        return results;
    }
    
    private static ReusableBuffer serializeVarLenPageBuf(List<byte[]> list) {
        
        int[] offsets = new int[list.size()];
        // List<Integer> offsets = new LinkedList<Integer>();
        int size = 0;
        int offsetPos = 0;
        for (byte[] buf : list) {
            size += buf.length;
            offsets[offsetPos] = size;
            offsetPos++;
        }
        
        size += list.size() * Integer.SIZE / 8;
        
        ReusableBuffer newBuf = BufferPool.allocate(size);
        for (byte[] buf : list)
            newBuf.put(buf);
        
        for (int offs : offsets)
            newBuf.putInt(offs);
        
        newBuf.position(0);
        
        return newBuf;
    }
    
    private static ReusableBuffer serializeVarLenPage(List<Object> list) {
        
        int[] offsets = new int[list.size()];
        // List<Integer> offsets = new LinkedList<Integer>();
        int size = 0;
        int offsetPos = 0;
        for (Object buf : list) {
            size += InternalBufferUtil.size(buf);
            offsets[offsetPos] = size;
            offsetPos++;
        }
        
        size += list.size() * Integer.SIZE / 8;
        
        ReusableBuffer newBuf = BufferPool.allocate(size);
        for (Object buf : list)
            newBuf.put(InternalBufferUtil.toBuffer(buf));
        
        for (int offs : offsets)
            newBuf.putInt(offs);
        
        newBuf.position(0);
        
        return newBuf;
    }
    
    private static ReusableBuffer serializeFixedLenPage(List<Object> list) {
        
        final int size = list.size() == 0 ? 0 : InternalBufferUtil.size(list.get(0)) * list.size();
        
        ReusableBuffer newBuf = BufferPool.allocate(size);
        for (Object buf : list)
            newBuf.put(InternalBufferUtil.toBuffer(buf));
        
        newBuf.position(0);
        
        return newBuf;
    }
    
    // private static SerializedPage serializeVarLenPage(List<Object> list) {
    //        
    // List<Object> offsetList = new LinkedList<Object>();
    //        
    // // append buffers containing all offsets at the end of the list
    // int size = 0;
    // for (Object buf : list) {
    // size += InternalBufferUtil.size(buf);
    // final ByteBuffer tmp = ByteBuffer.wrap(new byte[Integer.SIZE / 8]);
    // tmp.putInt(size);
    // offsetList.add(tmp.array());
    // }
    //        
    // size += list.size() * Integer.SIZE / 8;
    //        
    // return new SerializedPage(size, list, offsetList);
    // }
    //    
    // private static SerializedPage serializeFixedLenPage(List<Object> list) {
    //        
    // int size = 0;
    // for (Object buf : list)
    // size += InternalBufferUtil.size(buf);
    //        
    // return new SerializedPage(size, list);
    // }
    
}
