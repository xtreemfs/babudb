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

import org.xtreemfs.babudb.index.reader.CompressedBlockReader;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

public class CompressedBlockWriter implements BlockWriter {
    
    private List<byte[]> keys;
    
    private List<byte[]> values;
    
    private boolean      varLenKeys;
    
    private boolean      varLenVals;
    
    private byte[] prefix;
    
    public CompressedBlockWriter(boolean varLenKeys, boolean varLenVals) {
        
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
        List<byte[]> compressedKeys = compress(keys);
    	        
        ReusableBuffer keyBuf = varLenKeys ? serializeVarLenPage(compressedKeys)
            : serializeFixedLenPage(keys);
        ReusableBuffer valBuf = varLenVals ? serializeVarLenPage(values)
            : serializeFixedLenPage(values);
        
        int entries = keys.size();
        int keysOffset = CompressedBlockReader.PREFIX_OFFSET + this.prefix.length;
        int valsOffset = keysOffset + keyBuf.limit();
        
        ReusableBuffer returnBuf = BufferPool.allocate(valsOffset + valBuf.limit());
        /* the header consist of
         * 4 : ptr to vals
         * 4 : ptr to keys
         * 4 : number of entries
         * 4 : -1 => variable keys, or n => length of fixed size keys
         * 4 : -1 => variable values, or n => length of fixed size values
         * k : prefix
         * ... start of keys
         */
                
        returnBuf.putInt(valsOffset);
        returnBuf.putInt(keysOffset);
        returnBuf.putInt(entries);
        returnBuf.putInt(varLenKeys ? -1 : entries == 0 ? 0 : (keyBuf.limit() / entries));
        returnBuf.putInt(varLenVals ? -1 : entries == 0 ? 0 : (valBuf.limit() / entries));
        
        if(this.prefix.length > 0)
        	returnBuf.put(this.prefix);
        
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

    /**
     * Given a list of byte-arrays, compresses the entries and returns a 
     * list of compressed entries.
     * 
     * @param list
     * @return Compressed list
     */
    private List<byte[]> compress(List<byte[]> list) {
    	/* the input is assumed to be sorted 
    	 * - find the longest common prefix among the bytes
    	 * - record the prefix first in the new list
    	 * - subtract the prefix from each entry and write it out to the list 
    	 */
    	
    	List<byte[]> results = new LinkedList<byte[]>();
    	
    	/* special case when list has only one element 
    	 * this ensures that there will be no prefix
    	 */
    	if(list.size() == 1) {
    		this.prefix = new byte[0];
    		return list;
    	}
    	
    	byte[] prefix = list.get(0);
    	    	
    	/* find the longest common prefix (lcp) */
    	int longestPrefixLen = prefix.length; // cant be longer than prefix
    	
    	for(byte[] entry : list) {
    		int prefixLen = 0;
    		int prefixIndex = 0;
    		int entryIndex = 0;
    		int maxLen = Math.min(prefix.length, entry.length);
    		
    		while(prefixLen < maxLen && prefix[prefixIndex++] == entry[entryIndex++]) {
    			prefixLen++;
    		}
    		
    		if(prefixLen < longestPrefixLen) {
    			longestPrefixLen = prefixLen;
    		}
    	}

    	// Create the prefix
		byte[] LCP = new byte[longestPrefixLen];
		System.arraycopy(prefix, 0, LCP, 0, longestPrefixLen);
		this.prefix = LCP;

		// add the entries, removing the prefix
    	for(byte[] entry : list) {
    		if(longestPrefixLen <= 0) {
    			results.add(entry);
    		} else {
    			int newLen = entry.length - longestPrefixLen;
    			byte[] newEntry = new byte[newLen];
    			System.arraycopy(entry, longestPrefixLen, newEntry, 0, newLen);
    			results.add(newEntry);
    		}
    	}
		
    	return results;
    }
    
    private static ReusableBuffer serializeVarLenPage(List<byte[]> list) {
        
        int[] offsets = new int[list.size()];
//        List<Integer> offsets = new LinkedList<Integer>();
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
    
    private static ReusableBuffer serializeFixedLenPage(List<byte[]> list) {
        
        final int size = list.size() == 0 ? 0 : list.get(0).length * list.size();
        
        ReusableBuffer newBuf = BufferPool.allocate(size);
        for (byte[] buf : list)
            newBuf.put(buf);
        
        newBuf.position(0);
        
        return newBuf;
    }
    
}
