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

public class DefaultBlockReader implements BlockReader {
    
    public static final int     KEYS_OFFSET = 4 * Integer.SIZE / 8;
    
    private ByteBuffer          buffer;
    
    private int                 position;
    
    private int                 limit;
    
    private ByteRangeComparator comp;
    
    private MiniPage            keys;
    
    private MiniPage            values;
    
    private int                 numEntries;
    
    public DefaultBlockReader(ByteBuffer buf, int position, int limit, ByteRangeComparator comp) {
        
        this.buffer = buf;
        this.position = position;
        this.limit = limit;
        this.comp = comp;
        
        int keysOffset = position + KEYS_OFFSET;
        int valsOffset = position + buf.getInt(position);
        numEntries = buf.getInt(position + 4);
        int keyEntrySize = buf.getInt(position + 8);
        int valEntrySize = buf.getInt(position + 12);
        
        keys = keyEntrySize == -1 ? new VarLenMiniPage(numEntries, buf, keysOffset, valsOffset, comp)
            : new FixedLenMiniPage(keyEntrySize, numEntries, buf, keysOffset, valsOffset, comp);
        values = valEntrySize == -1 ? new VarLenMiniPage(numEntries, buf, valsOffset, limit, comp)
            : new FixedLenMiniPage(valEntrySize, numEntries, buf, valsOffset, limit, comp);
    }
    
    public BlockReader clone() {
        buffer.position(0);
        return new DefaultBlockReader(buffer.slice(), position, limit, comp);
    }
    
    /* (non-Javadoc)
	 * @see org.xtreemfs.babudb.index.reader.BlockReader#lookup(byte[])
	 */
    public ByteRange lookup(byte[] key) {
        
        int index = keys.getPosition(key);
        if (index == -1)
            return null;
        
        return values.getEntry(index);
    }
    
    /* (non-Javadoc)
	 * @see org.xtreemfs.babudb.index.reader.BlockReader#rangeLookup(byte[], byte[], boolean)
	 */
    public Iterator<Entry<ByteRange, ByteRange>> rangeLookup(byte[] from, byte[] to, final boolean ascending) {
        
        final int startIndex;
        final int endIndex;
        
        {
            startIndex = keys.getTopPosition(from);
            assert (startIndex >= -1) : "invalid block start offset: " + startIndex;
            
            endIndex = keys.getBottomPosition(to);
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
    
    /* (non-Javadoc)
	 * @see org.xtreemfs.babudb.index.reader.BlockReader#getKeys()
	 */
    public MiniPage getKeys() {
        return keys;
    }
    
    /* (non-Javadoc)
	 * @see org.xtreemfs.babudb.index.reader.BlockReader#getValues()
	 */
    public MiniPage getValues() {
        return values;
    }
    
    /* (non-Javadoc)
	 * @see org.xtreemfs.babudb.index.reader.BlockReader#getNumEntries()
	 */
    public int getNumEntries() {
        return numEntries;
    }
    
}
