/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.babudb.index.ByteRangeComparator;

public class CompressedBlockReader implements BlockReader {
    
    public static final int     PREFIX_OFFSET = 5 * Integer.SIZE / 8;
    
    private ByteBuffer          buffer;
    
    private int                 position;
    
    private int                 limit;
    
    private ByteRangeComparator comp;
    
    private MiniPage            keys;
    
    private MiniPage            values;
    
    private int                 numEntries;

    private byte[]				prefix;
    
    public CompressedBlockReader(ByteBuffer buf, int position, int limit, ByteRangeComparator comp) {
        
        this.buffer = buf;
        this.position = position;
        this.limit = limit;
        this.comp = comp;
        
        int valsOffset = position + buf.getInt(position);
        int keysOffset = position + buf.getInt(position + 4);
        numEntries = buf.getInt(position + 8);
        int keyEntrySize = buf.getInt(position + 12);
        int valEntrySize = buf.getInt(position + 16);
        
        // read in the prefix string
        int prefixSize = (keysOffset - position) - PREFIX_OFFSET;
        prefix = new byte[prefixSize];
        
        if(prefixSize > 0) {
        	// move to the position to perform the read
        	buf.position(position + PREFIX_OFFSET);
        	buf.get(prefix);
        	// reset to original position
        	buf.position(position);
        }
        
        keys = keyEntrySize == -1 ? new VarLenMiniPage(numEntries, buf, keysOffset, valsOffset, comp)
            : new FixedLenMiniPage(keyEntrySize, numEntries, buf, keysOffset, valsOffset, comp);
        values = valEntrySize == -1 ? new VarLenMiniPage(numEntries, buf, valsOffset, limit, comp)
            : new FixedLenMiniPage(valEntrySize, numEntries, buf, valsOffset, limit, comp);
    }
    
    public CompressedBlockReader clone() {
        buffer.position(0);
        return new CompressedBlockReader(buffer.slice(), position, limit, comp);
    }
    
    /**
     * Returns null if the key is not matching the block prefix, 
     * otherwise the suffix is returned, i.e. key - prefix.
     * @param key
     * @return byte[] suffix of key.
     */
    private byte[] usableSuffix(byte[] key) {
    	// key cant contain the prefix
    	if(key == null || prefix.length > key.length)
    		return null;

    	// no prefix exist, use the key as is
    	if(prefix.length == 0)
    		return key;
    		
    	byte[] prefixKey = new byte[prefix.length];
    	System.arraycopy(key, 0, prefixKey, 0, prefix.length);
    	
    	if(comp.compare(prefix, prefixKey) != 0)
    		return null;

    	byte[] suffixKey = new byte[key.length - prefix.length];
		System.arraycopy(key, prefix.length, suffixKey, 0, key.length - prefix.length);
		
    	return suffixKey;
    }

    public ByteRange lookup(byte[] key) {
    	// if the key contains prefix check if the block 
    	// contains what remains after removing the prefix

    	byte[] suffixKey = usableSuffix(key);

    	if(suffixKey == null)
    		return null;
    	
    	int index = keys.getPosition(suffixKey);

    	if (index == -1)
    		return null;
        
    	return values.getEntry(index);
    }
    
    public Iterator<Entry<ByteRange, ByteRange>> rangeLookup(byte[] from, byte[] to, final boolean ascending) {
        
        final int startIndex;
        final int endIndex;
        
        {
        	byte[] suffixFrom = usableSuffix(from);
            startIndex = keys.getTopPosition(suffixFrom);
            assert (startIndex >= -1) : "invalid block start offset: " + startIndex;
            
        	byte[] suffixTo = usableSuffix(to);
            endIndex = ascending ? keys.getExclBottomPosition(suffixTo) : keys.getInclBottomPosition(suffixTo);
            assert (endIndex >= -1) : "invalid block end offset: " + endIndex;
        }
        
        return new Iterator<Entry<ByteRange, ByteRange>>() {
            
            int currentIndex = ascending ? startIndex : endIndex;
            
            @Override
            public boolean hasNext() {
                return ascending ? currentIndex <= endIndex : currentIndex >= startIndex;
            }
            
            @Override
            public Entry<ByteRange, ByteRange> next() {
                
                if (!hasNext())
                    throw new NoSuchElementException();
                
                Entry<ByteRange, ByteRange> entry = new Entry<ByteRange, ByteRange>() {
                    
                    final ByteRange key   = keys.getEntry(currentIndex);
                    
                    final ByteRange value = values.getEntry(currentIndex);
                    
                    @Override
                    public ByteRange getValue() {
                        return value;
                    }
                    
                    @Override
                    public ByteRange getKey() {
                    	key.addPrefix(prefix);
                        return key;
                    }
                    
                    @Override
                    public ByteRange setValue(ByteRange value) {
                        throw new UnsupportedOperationException();
                    }
                };
                
                if (ascending)
                    currentIndex++;
                else
                    currentIndex--;
                
                return entry;
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
    
    public MiniPage getKeys() {
        return keys;
    }
    
    public MiniPage getValues() {
        return values;
    }
    
    public int getNumEntries() {
        return numEntries;
    }
    
}
