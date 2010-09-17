/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.foundation.buffer.BufferPool;

public class DefaultBlockReader extends BlockReader {
    
    public static final int KEYS_OFFSET = 4 * Integer.SIZE / 8;
    
    /**
     * Creates a reader for a buffered block.
     * 
     * @param buf
     *            the buffer
     * @param position
     *            the position of the block in the buffer
     * @param limit
     *            the limit of the block in the buffer
     * @param comp
     *            the byte range comparator
     */
    public DefaultBlockReader(ByteBuffer buf, int position, int limit, ByteRangeComparator comp) {
        
        super(true);
        
        this.buffer = buf;
        this.position = position;
        this.limit = limit;
        this.comp = comp;
        
        // with limit <= 0 there are no entries in the buffer
        if (limit > 0) {
            int keysOffset = position + KEYS_OFFSET;
            int valsOffset = position + buf.getInt(position);
            numEntries = buf.getInt(position + 4);
            int keyEntrySize = buf.getInt(position + 8);
            int valEntrySize = buf.getInt(position + 12);
            keys = keyEntrySize == -1 ? new VarLenMiniPage(numEntries, buf, keysOffset, valsOffset, comp)
                : new FixedLenMiniPage(keyEntrySize, numEntries, buf, keysOffset, valsOffset, comp);
            values = valEntrySize == -1 ? new VarLenMiniPage(numEntries, buf, valsOffset, limit, comp)
                : new FixedLenMiniPage(valEntrySize, numEntries, buf, valsOffset, limit, comp);
        } else {
            numEntries = 0;
            keys = new FixedLenMiniPage(0, 0, null, 0, 0, comp);
            values = new FixedLenMiniPage(0, 0, null, 0, 0, comp);
        }
        
    }
    
    /**
     * Creates a reader for a streamed block.
     * 
     * @param channel
     *            the channel to the block file
     * @param position
     *            the position of the block
     * @param limit
     *            the limit of the block
     * @param comp
     *            the byte range comparator
     */
    public DefaultBlockReader(FileChannel channel, int position, int limit, ByteRangeComparator comp)
        throws IOException {
        
        super(false);
        
        this.position = position;
        this.limit = limit;
        this.comp = comp;

        this.readBuffer = BufferPool.allocate(limit - position);
        channel.read(readBuffer.getBuffer(), position);
        
        // with limit <= 0 there are no entries in the buffer
        if (limit > 0) {
            int keysOffset = KEYS_OFFSET;
            int valsOffset = readBuffer.getBuffer().getInt(0);
            numEntries = readBuffer.getBuffer().getInt(4);
            int keyEntrySize = readBuffer.getBuffer().getInt(8);
            int valEntrySize = readBuffer.getBuffer().getInt(12);
            keys = keyEntrySize == -1 ? new VarLenMiniPage(numEntries, readBuffer.getBuffer(), keysOffset,
                valsOffset, comp) : new FixedLenMiniPage(keyEntrySize, numEntries, readBuffer.getBuffer(),
                keysOffset, valsOffset, comp);
            values = valEntrySize == -1 ? new VarLenMiniPage(numEntries, readBuffer.getBuffer(), valsOffset,
                limit - position, comp) : new FixedLenMiniPage(valEntrySize, numEntries, readBuffer
                    .getBuffer(), valsOffset, limit - position, comp);
        } else {
            numEntries = 0;
            keys = new FixedLenMiniPage(0, 0, null, 0, 0, comp);
            values = new FixedLenMiniPage(0, 0, null, 0, 0, comp);
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.index.reader.BlockReader#lookup(byte[])
     */
    public ByteRange lookup(byte[] key) {
        
        int index = keys.getPosition(key);
        if (index == -1)
            return null;
        
        return values.getEntry(index);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.index.reader.BlockReader#rangeLookup(byte[],
     * byte[], boolean)
     */
    public Iterator<Entry<ByteRange, ByteRange>> rangeLookup(byte[] from, byte[] to, final boolean ascending) {
        
        final int startIndex;
        final int endIndex;
        
        {
            startIndex = keys.getTopPosition(from);
            assert (startIndex >= -1) : "invalid block start offset: " + startIndex;
            
            endIndex = ascending ? keys.getExclBottomPosition(to) : keys.getInclBottomPosition(to);
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
    
}
