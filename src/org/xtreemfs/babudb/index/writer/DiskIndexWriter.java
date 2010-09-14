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

import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Writes an index to a set of files on disk. A file will not be larger than the
 * given max file size. Multiple files are necessary to handle databases larger
 * than 2 GB on 32-bit systems due to MMap limitations.
 * 
 * The index has two parts, a sorted list of blocks containing key/value-pairs
 * and a block index. The block index is a sparse index pointing to the sorted
 * blocks.
 * 
 * @author stender
 * @author hoegqvist
 */
public class DiskIndexWriter {
    
    private String  path;
    
    private int     maxBlockEntries;
    
    private boolean compressed;
    
    private int     maxFileSize;
    
    private short   blockFileId = 0;
    
    /**
     * Creates a new DiskIndexWriter
     * 
     * @param path
     *            The path to the directory where the index will be written. The
     *            directory is created if it does not yet exist.
     * @param maxBlockEntries
     *            The maximum number of entries in a single block.
     * @param compressed
     *            Indicates if the blocks should be compressed.
     * @param maxFileSize
     *            The max size of a file storing blocks in bytes. On a 32-bit
     *            system this should not be larger than 2GB.
     * @throws IOException
     */
    public DiskIndexWriter(String path, int maxBlockEntries, boolean compressed, int maxFileSize)
        throws IOException {
        
        if (!path.endsWith(System.getProperty("file.separator")))
            path += System.getProperty("file.separator");
        
        // check that the file-size will be accepted by MMap
        assert (maxFileSize <= Integer.MAX_VALUE);
        
        File diDir = new File(path);
        
        if (diDir.exists())
            throw new IOException("index already exists");
        
        // make sure that the path is a directory and that it exists
        if (!diDir.mkdirs())
            throw new IOException("could not create directory '" + path + "'");
        
        this.compressed = compressed;
        
        this.path = path;
        this.maxBlockEntries = maxBlockEntries;
        this.maxFileSize = maxFileSize;
    }
    
    /**
     * Write blocks to the file at the given path until the maxFileSize is
     * reached.
     * 
     * @param path
     * @param iterator
     * @throws IOException
     */
    private void writeIndex(String path, BlockWriter blockIndex, Iterator<Entry<byte[], byte[]>> iterator)
        throws IOException {
        FileOutputStream fout = new FileOutputStream(path);
        FileChannel channel = fout.getChannel();
        channel.truncate(0);
        
        BlockWriter block;
        
        if (compressed)
            block = new CompressedBlockWriter(true, true);
        else
            block = new DefaultBlockWriter(true, true);
        
        int entryCount = 0;
        int blockOffset = 0;
        boolean newBlockFile = false;
        
        // write each block to disk
        // note that blocks can become slightly larger than the maxFileSize
        // depending on the size of the last block
        while (iterator.hasNext() && !newBlockFile) {
            
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
                ReusableBuffer buf = ReusableBuffer.wrap(new byte[(Integer.SIZE / 8) + (Short.SIZE / 8)]);
                buf.putInt(blockOffset);
                buf.putShort(blockFileId);
                
                // add the key-offset mapping to the block index
                blockIndex.add(block.getBlockKey(), buf.array());
                
                // serialize the block
                ReusableBuffer serializedBlock = block.serialize();
                blockOffset += serializedBlock.limit();
                
                // provide for robustness, as channels sometimes seem to be
                // closed by previous writes
                if (!channel.isOpen()) {
                    Logging.logMessage(Logging.LEVEL_WARN, this,
                        "channel was closed while writing checkpoint, re-opening channel");
                    fout.close();
                    fout = new FileOutputStream(path, true);
                    channel = fout.getChannel();
                }
                
                // write the block
                int count = channel.write(serializedBlock.getBuffer());
                assert (count == serializedBlock.limit());
                BufferPool.free(serializedBlock);
                
                if (blockOffset >= maxFileSize) {
                    newBlockFile = true;
                } else {
                    if (iterator.hasNext())
                        if (compressed)
                            block = new CompressedBlockWriter(true, true);
                        else
                            block = new DefaultBlockWriter(true, true);
                }
            }
            
        }
        
        channel.close();
        fout.close();
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
        
        new File(path + "blockindex.idx").createNewFile();
        FileChannel channel = new FileOutputStream(path + "blockindex.idx").getChannel();
        channel.truncate(0);
        BlockWriter blockIndex = new DefaultBlockWriter(true, false);
        
        while (iterator.hasNext()) {
            String indexPath = path + "blockfile_" + new Short(blockFileId).toString() + ".idx";
            writeIndex(indexPath, blockIndex, iterator);
            
            blockFileId += 1;
            
            // write the block index offset
            // ReusableBuffer buf = BufferPool.allocate(Integer.SIZE / 8);
            // buf.putInt(blockOffset);
            // buf.position(0);
            // channel.write(buf.getBuffer());
            // BufferPool.free(buf);
        }
        
        // write the block index to disk
        ReusableBuffer serializedBuf = blockIndex.serialize();
        channel.write(serializedBuf.getBuffer());
        BufferPool.free(serializedBuf);
        
        channel.close();
    }
    
}
