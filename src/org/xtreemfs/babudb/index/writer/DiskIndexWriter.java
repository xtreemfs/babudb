/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.index.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * Writes an index to a file on disk.
 * 
 * @author stender
 * 
 */
public class DiskIndexWriter {
    
    private String 	path;
    
    private int    	maxBlockEntries;
    
    private boolean compressed;
    
    public DiskIndexWriter(String path, int maxBlockEntries, boolean compressed) throws IOException {

        if (new File(path).exists())
            throw new IOException("index already exists");

    	this.compressed = compressed;
    	
        this.path = path;
        this.maxBlockEntries = maxBlockEntries;
    }
    
    /**
     * Creates an on-disk representation of an index from an iterator of
     * key-value pairs. The iterator has to return keys in ascending order!
     * 
     * @param iterator
     *            an iterator w/ key-value pairs, keys must be in ascending
     *            order
     * @throws IOException
     *             if an I/O error occurs
     */
    public void writeIndex(Iterator<Entry<byte[], byte[]>> iterator) throws IOException {
        
        FileChannel channel = new FileOutputStream(path).getChannel();
        channel.truncate(0);
        
        BlockWriter blockIndex = new DefaultBlockWriter(true, false);
        BlockWriter block;
        
        if(compressed)
        	block = new CompressedBlockWriter(true, true);
        else
        	block = new DefaultBlockWriter(true, true);
        
        int entryCount = 0;
        int blockOffset = 0;
        
        // write each block to disk
        while (iterator.hasNext()) {
            
            // add the next key-value pair to the current block
            Entry<byte[], byte[]> keyValuePair = iterator.next();
            byte[] key = keyValuePair.getKey();
            byte[] val = keyValuePair.getValue();
            block.add(key, val);
            
            entryCount++;
            
            // if the block size limit has been reached, or there are no more
            // key-value pairs, serialize the block and write it to disk
            if (entryCount % maxBlockEntries == 0 || !iterator.hasNext()) {
                
                // serialize the offset of the block into a new buffer
                ReusableBuffer buf = ReusableBuffer.wrap(new byte[Integer.SIZE / 8]);
                buf.putInt(blockOffset);
                
                // add the key-offset mapping to the block index
                blockIndex.add(block.getBlockKey(), buf.array());
                
                // serialize the block
                ReusableBuffer serializedBlock = block.serialize();
                blockOffset += serializedBlock.limit();
                
                // write the block
                channel.write(serializedBlock.getBuffer());
                BufferPool.free(serializedBlock);
                
                if (iterator.hasNext())
                    if(compressed)
                    	block = new CompressedBlockWriter(true, true);
                    else
                    	block = new DefaultBlockWriter(true, true);
            }
            
        }
        
        // write the block index to disk
        ReusableBuffer serializedBuf = blockIndex.serialize();
        channel.write(serializedBuf.getBuffer());
        BufferPool.free(serializedBuf);
        
        // write the block index offset
        ReusableBuffer buf = BufferPool.allocate(Integer.SIZE / 8);
        buf.putInt(blockOffset);
        buf.position(0);
        channel.write(buf.getBuffer());
        BufferPool.free(buf);
        
        channel.close();
    }
    
}
