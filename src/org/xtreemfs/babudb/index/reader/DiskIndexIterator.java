/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.foundation.buffer.BufferPool;

public class DiskIndexIterator extends DiskIndexIteratorBase implements Iterator<Entry<byte[], byte[]>> {
    
    /**
     * Disk index iterator for mmap'ed index files.
     * 
     * @param index
     *            reference to the index
     * @param blockIndexReader
     *            reference to the block index reader
     * @param from
     *            smallest key (inclusively)
     * @param to
     *            largest key (exclusively)
     * @param ascending
     *            defines the iteration order
     * @param maps
     *            an array of mmap'ed buffers
     */
    public DiskIndexIterator(DiskIndex index, BlockReader blockIndexReader, byte[] from, byte[] to,
        boolean ascending, ByteBuffer[] maps) {
        super(index, blockIndexReader, from, to, ascending, maps, null);
    }
    
    /**
     * Disk index iterator for streamed index files.
     * 
     * @param index
     *            reference to the index
     * @param blockIndexReader
     *            reference to the block index reader
     * @param from
     *            smallest key (inclusively)
     * @param to
     *            largest key (exclusively)
     * @param ascending
     *            defines the iteration order
     * @param dbFileChannels
     *            an array of file channels
     */
    public DiskIndexIterator(DiskIndex index, BlockReader blockIndexReader, byte[] from, byte[] to,
        boolean ascending, FileChannel[] dbFileChannels) {
        super(index, blockIndexReader, from, to, ascending, null, dbFileChannels);
    }
    
    @Override
    public Entry<byte[], byte[]> next() {
        
        if (!hasNext())
            throw new NoSuchElementException();
        
        final Entry<ByteRange, ByteRange> entry = currentBlockIterator.next();
        return new Entry<byte[], byte[]>() {
            
            private byte[] key;
            
            private byte[] value;
            
            {
                key = entry.getKey().toBuffer();
                value = entry.getValue().toBuffer();
                
                if(entry.getValue().getReusableBuf() != null)
                    BufferPool.free(entry.getValue().getReusableBuf());
            }
            
            @Override
            public byte[] getKey() {
                return key;
            }
            
            @Override
            public byte[] getValue() {
                return value;
            }
            
            @Override
            public byte[] setValue(byte[] value) {
                throw new UnsupportedOperationException();
            }
            
        };
    }
    
}
