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
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.index.ByteRange;

public class InternalDiskIndexIterator extends DiskIndexIteratorBase implements
    ResultSet<ByteRange, ByteRange> {
    
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
    public InternalDiskIndexIterator(DiskIndex index, BlockReader blockIndexReader, byte[] from, byte[] to,
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
    public InternalDiskIndexIterator(DiskIndex index, BlockReader blockIndexReader, byte[] from, byte[] to,
        boolean ascending, FileChannel[] dbFileChannels) {
        super(index, blockIndexReader, from, to, ascending, null, dbFileChannels);
    }
    
    @Override
    public Entry<ByteRange, ByteRange> next() {
        
        if (!hasNext())
            throw new NoSuchElementException();
        
        final Entry<ByteRange, ByteRange> entry = currentBlockIterator.next();
        
        return new Entry<ByteRange, ByteRange>() {
            
            private ByteRange key;
            
            private ByteRange value;
            
            {
                key = entry.getKey();
                value = entry.getValue();
            }
            
            @Override
            public ByteRange getKey() {
                return key;
            }
            
            @Override
            public ByteRange getValue() {
                return value;
            }
            
            @Override
            public ByteRange setValue(ByteRange value) {
                throw new UnsupportedOperationException();
            }
            
        };
    }
    
}
