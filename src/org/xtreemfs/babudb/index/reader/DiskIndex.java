/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.index.reader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.babudb.index.ByteRangeComparator;

public class DiskIndex {
    
    private ByteBuffer          blockIndexBuf;
    
    private BlockReader         blockIndex;
    
    private ByteBuffer          mappedFile;
    
    private RandomAccessFile    dbFile;
    
    private int                 blockIndexOffset;
    
    private ByteRangeComparator comp;
    
    public DiskIndex(String path, ByteRangeComparator comp) throws IOException {
        
        this.comp = comp;
        
        // First, read the block index into a buffer. For performance reasons,
        // the block index has to remain in memory all the time, so it cannot be
        // memory-mapped.
        
        dbFile = new RandomAccessFile(path, "r");
        dbFile.seek(dbFile.length() - Integer.SIZE / 8);
        blockIndexOffset = dbFile.readInt();
        
        blockIndexBuf = ByteBuffer
                .allocate((int) (dbFile.length() - Integer.SIZE / 8 - blockIndexOffset));
        FileChannel channel = dbFile.getChannel();
        channel.position(blockIndexOffset);
        channel.read(blockIndexBuf);
        
        blockIndex = new BlockReader(blockIndexBuf, 0, blockIndexBuf.limit(), comp);
        
        // Second, mmap the potentially huge 'blocks' region.
        
        mappedFile = channel.map(MapMode.READ_ONLY, 0, blockIndexOffset);
        assert (channel.size() <= Integer.MAX_VALUE);
    }
    
    public byte[] lookup(byte[] key) {
        
        int indexPosition = getBlockIndexPosition(key, blockIndex);
        
        // if the first element is larger than the key searched for, the key is
        // not contained in the index
        if (indexPosition == -1)
            return null;
        
        int startBlockOffset = getBlockOffset(indexPosition, blockIndex);
        
        int endBlockOffset = -1;
        if (indexPosition == blockIndex.getNumEntries() - 1)
            endBlockOffset = blockIndexOffset;
        else
            endBlockOffset = getBlockOffset(indexPosition + 1, blockIndex);
        
        // create a view buffer on the target block
        BlockReader targetBlock = getBlock(startBlockOffset, endBlockOffset, mappedFile);
        
        // search for the key in the target block and return the result
        ByteRange val = targetBlock.lookup(key);
        return val == null ? null : val.toBuffer();
    }
    
    public Iterator<Entry<byte[], byte[]>> rangeLookup(final byte[] from, final byte[] to) {
        
        final BlockReader itBlockIndex = blockIndex.clone();
        mappedFile.position(0);
        final ByteBuffer map = mappedFile.slice();
        
        final int blockIndexStart = from == null ? 0 : getBlockIndexPosition(from, itBlockIndex);
        final int blockIndexEnd = to == null ? itBlockIndex.getNumEntries() - 1
            : getBlockIndexPosition(to, itBlockIndex);
        
        return new Iterator<Entry<byte[], byte[]>>() {
            
            private int                                   currentBlockIndex;
            
            private Iterator<Entry<ByteRange, ByteRange>> currentBlockIterator;
            
            private BlockReader                           currentBlock;
            
            {
                currentBlockIndex = blockIndexStart;
                getNextBlockData();
            }
            
            @Override
            public boolean hasNext() {
                
                while (currentBlockIterator != null) {
                    
                    if (currentBlockIterator.hasNext())
                        return true;
                    
                    currentBlockIndex++;
                    getNextBlockData();
                }
                
                return false;
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
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
            private void getNextBlockData() {
                
                if (currentBlockIndex > blockIndexEnd) {
                    currentBlock = null;
                    currentBlockIterator = null;
                    return;
                }
                
                int startOffset = getBlockOffset(currentBlockIndex, itBlockIndex);
                int endOffset = currentBlockIndex == itBlockIndex.getNumEntries() - 1 ? blockIndexOffset
                    : getBlockOffset(currentBlockIndex + 1, itBlockIndex);
                
                currentBlock = getBlock(startOffset, endOffset, map);
                currentBlockIterator = currentBlock == null ? null : currentBlock.rangeLookup(
                    from == null ? null : from, to == null ? null : to);
            }
            
        };
    }
    
    public void destroy() throws IOException {
        dbFile.close();
    }
    
    private BlockReader getBlock(int startBlockOffset, int endBlockOffset, ByteBuffer map) {
        
        if (startBlockOffset > map.limit())
            return null;
        
        if (endBlockOffset == -1)
            endBlockOffset = map.limit();
        
        BlockReader targetBlock = new BlockReader(map, startBlockOffset, endBlockOffset, comp);
        return targetBlock;
    }
    
    /**
     * Returns the index of the block potentially contains the given key.
     * 
     * @param key
     *            the key for which to find the block index
     * @param index
     *            the block index
     * @return the block index
     */
    private int getBlockIndexPosition(byte[] key, BlockReader index) {
        return SearchUtil.getInclBottomOffset(index.getKeys(), key, comp);
    }
    
    /**
     * Returns the offset at which the block with the given index position
     * starts.
     * 
     * @param indexPosition
     *            the index position
     * @param index
     *            the block index
     * @return the offset
     */
    private static int getBlockOffset(int indexPosition, BlockReader index) {
        ByteRange range = index.getValues().getEntry(indexPosition);
        return range.getBuf().getInt(range.getStartOffset());
    }
    
}
