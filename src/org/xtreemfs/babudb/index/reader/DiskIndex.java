/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.foundation.logging.Logging;

public class DiskIndex {
    
    private ByteBuffer          blockIndexBuf;
    
    private BlockReader         blockIndex;
    
    private MappedByteBuffer[]  dbFiles;
    
    private FileChannel[]       dbFileChannels;
    
    private ByteRangeComparator comp;
    
    private long                indexSize;
    
    private final boolean       compressed;
    
    private final boolean       mmaped;
    
    public DiskIndex(String path, ByteRangeComparator comp, boolean compressed, boolean mmaped)
        throws IOException {
        if (!path.endsWith(System.getProperty("file.separator")))
            path += System.getProperty("file.separator");
        
        if (!new File(path).exists())
            throw new IOException("There is no index at " + path);
        
        this.comp = comp;
        this.compressed = compressed;
        this.mmaped = mmaped;
        Logging.logMessage(Logging.LEVEL_INFO, this, "loading index ...");
        
        // First, read the block index into a buffer. For performance reasons,
        // the block index has to remain in memory all the time, so it cannot be
        // memory-mapped.
        
        RandomAccessFile blockIndexFile = new RandomAccessFile(path + "blockindex.idx", "r");
        blockIndexBuf = ByteBuffer.allocate((int) (blockIndexFile.length()));
        FileChannel channel = blockIndexFile.getChannel();
        channel.read(blockIndexBuf);
        
        blockIndex = new DefaultBlockReader(blockIndexBuf, 0, blockIndexBuf.limit(), comp);
        channel.close();
        
        // Second, mmap each of the potentially large block list files
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String filename) {
                return filename.startsWith("blockfile_");
            }
        };
        String blockFilenames[] = new File(path).list(filter);
        
        Pattern p = Pattern.compile("blockfile_(\\d+).idx");
        
        dbFileChannels = new FileChannel[blockFilenames.length];
        
        if (mmaped)
            dbFiles = new MappedByteBuffer[blockFilenames.length];
        
        for (String blockFilename : blockFilenames) {
            Matcher m = p.matcher(blockFilename);
            if (m.matches()) {
                int blockIndexId = new Integer(m.group(1)).intValue();
                RandomAccessFile blockFile = new RandomAccessFile(path + blockFilename, "r");
                dbFileChannels[blockIndexId] = blockFile.getChannel();
                indexSize += blockFile.length();
                
                // if mmap'ed access is used, map the index files and close the
                // channels; otherwise, no maps will be created, and channels
                // will be closed when the index is released
                if (mmaped) {
                    dbFiles[blockIndexId] = dbFileChannels[blockIndexId].map(MapMode.READ_ONLY, 0, blockFile
                            .length());
                    Logging.logMessage(Logging.LEVEL_INFO, this, "block file index size: "
                        + blockFile.length());
                    dbFileChannels[blockIndexId].close();
                }
                
            }
        }
        
    }
    
    public byte[] lookup(byte[] key) {
        // returns index position in the second block for "word"
        int indexPosition = getBlockIndexPosition(key, blockIndex);
        
        // if the first element is larger than the key searched for, the key is
        // not contained in the index
        if (indexPosition == -1)
            return null;
        
        int startBlockOffset = getBlockOffset(indexPosition, blockIndex);
        int fileId = getBlockFileId(indexPosition, blockIndex);
        
        int endBlockOffset;
        if (indexPosition == blockIndex.getNumEntries() - 1)
            // the last block in the block index
            endBlockOffset = -1;
        else {
            ByteRange indexPos = getBlockEntry(indexPosition + 1, blockIndex);
            ByteBuffer indexPosBuf = indexPos.getBuf();
            endBlockOffset = getBlockIndexOffset(indexPosBuf, indexPos.getStartOffset());
            
            // is this the last block of the current block file?
            // then the endBlockOffset should be set to the end of the file
            if (getBlockIndexFileId(indexPosBuf, indexPos.getStartOffset()) > fileId)
                endBlockOffset = -1;
            
            // endBlockOffset = getBlockOffset(indexPosition + 1, blockIndex);
        }
        
        // create a view buffer on the target block
        BlockReader targetBlock = null;
        try {
            targetBlock = mmaped ? getBlock(startBlockOffset, endBlockOffset, dbFiles[fileId]) : getBlock(
                startBlockOffset, endBlockOffset, dbFileChannels[fileId]);
        } catch (IOException e) {
            Logging.logError(Logging.LEVEL_ERROR, this, e);
        }
        
        // search for the key in the target block and return the result
        ByteRange val = targetBlock.lookup(key);
        targetBlock.free();
        return val == null ? null : val.toBuffer();
    }
    
    public long numKeys() {
        
        int numBlocks = blockIndex.getNumEntries();
        
        // return 0 if no keys are contained
        if (numBlocks == 0)
            return 0;
        
        int lastBlockStartOffset = getBlockOffset(numBlocks - 1, blockIndex);
        int lastBlockEndOffset = -1;
        BlockReader lastBlock = null;
        try {
            lastBlock = mmaped ? getBlock(lastBlockStartOffset, lastBlockEndOffset, dbFiles[getBlockFileId(
                numBlocks - 1, blockIndex)]) : getBlock(lastBlockStartOffset, lastBlockEndOffset,
                dbFileChannels[getBlockFileId(numBlocks - 1, blockIndex)]);
        } catch (IOException e) {
            Logging.logError(Logging.LEVEL_ERROR, this, e);
        }
        long lastBlockEntryCount = lastBlock.getNumEntries();
        lastBlock.free();
        
        if (numBlocks == 1)
            return lastBlockEntryCount;
        
        int firstBlockStartOffset = 0;
        int firstBlockEndBlockOffset = getBlockOffset(1, blockIndex);
        
        // the 1 block is the last in the current block file
        if (getBlockFileId(1, blockIndex) > getBlockFileId(0, blockIndex))
            firstBlockEndBlockOffset = -1;
        
        BlockReader firstBlock = null;
        try {
            firstBlock = mmaped ? getBlock(firstBlockStartOffset, firstBlockEndBlockOffset,
                dbFiles[getBlockFileId(0, blockIndex)]) : getBlock(firstBlockStartOffset,
                firstBlockEndBlockOffset, dbFileChannels[getBlockFileId(0, blockIndex)]);
        } catch (IOException e) {
            Logging.logError(Logging.LEVEL_ERROR, this, e);
        }
        long firstBlocksEntryCount = (long) firstBlock.getNumEntries() * (numBlocks - 1);
        firstBlock.free();
        
        return firstBlocksEntryCount + lastBlockEntryCount;
    }
    
    public Iterator<Entry<byte[], byte[]>> rangeLookup(final byte[] from, final byte[] to,
        final boolean ascending) {
        
        // return iterator for mmap'ed indices
        if (mmaped) {
            final ByteBuffer[] map = new ByteBuffer[dbFiles.length];
            for (int i = 0; i < dbFiles.length; i++) {
                dbFiles[i].position(0);
                map[i] = dbFiles[i].slice();
            }
            
            return new DiskIndexIterator(this, blockIndex, from, to, ascending, map);
        }
        
        // return iterator for non-mmap'ed indices
        else
            return new DiskIndexIterator(this, blockIndex, from, to, ascending, dbFileChannels);
    }
    
    public Iterator<Entry<ByteRange, ByteRange>> internalRangeLookup(final byte[] from, final byte[] to,
        final boolean ascending) {
        
        // return iterator for mmap'ed indices
        if (mmaped) {
            final ByteBuffer[] map = new ByteBuffer[dbFiles.length];
            for (int i = 0; i < dbFiles.length; i++) {
                dbFiles[i].position(0);
                map[i] = dbFiles[i].slice();
            }
            
            return new InternalDiskIndexIterator(this, blockIndex, from, to, ascending, map);
        }
        
        // return iterator for non-mmap'ed indices
        else
            return new InternalDiskIndexIterator(this, blockIndex, from, to, ascending, dbFileChannels);
    }
    
    public ByteRangeComparator getComparator() {
        return comp;
    }
    
    public long getSize() {
        return indexSize;
    }
    
    public void destroy() throws IOException {
        // for (FileChannel c : dbFileChannels) {
        // c.close();
        // }
    }
    
    public void finalize() {
        for (FileChannel c : dbFileChannels) {
            try {
                c.close();
            } catch (IOException exc) {
                Logging.logError(Logging.LEVEL_ERROR, this, exc);
            }
        }
    }
    
    protected BlockReader getBlock(int startBlockOffset, int endBlockOffset, ByteBuffer map) {
        
        if (startBlockOffset > map.limit())
            return null;
        
        if (endBlockOffset == -1)
            endBlockOffset = map.limit();
        
        BlockReader targetBlock;
        
        if (compressed) {
            targetBlock = new CompressedBlockReader(map, startBlockOffset, endBlockOffset, comp);
        } else {
            targetBlock = new DefaultBlockReader(map, startBlockOffset, endBlockOffset, comp);
        }
        
        return targetBlock;
    }
    
    protected BlockReader getBlock(int startBlockOffset, int endBlockOffset, FileChannel channel)
        throws IOException {
        
        if (startBlockOffset > channel.size())
            return null;
        
        if (endBlockOffset == -1)
            endBlockOffset = (int) channel.size();
        
        BlockReader targetBlock;
        
        if (compressed) {
            targetBlock = new CompressedBlockReader(channel, startBlockOffset, endBlockOffset, comp);
        } else {
            targetBlock = new DefaultBlockReader(channel, startBlockOffset, endBlockOffset, comp);
        }
        
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
    protected int getBlockIndexPosition(byte[] key, BlockReader index) {
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
    protected static int getBlockOffset(int indexPosition, BlockReader index) {
        ByteRange range = index.getValues().getEntry(indexPosition);
        return range.getBuf().getInt(range.getStartOffset());
    }
    
    /**
     * Returns the id of the block file
     * 
     * @param indexPosition
     *            the position in the block index
     * @param index
     *            the block index
     * @return the block file id
     */
    protected static short getBlockFileId(int indexPosition, BlockReader index) {
        ByteRange range = index.getValues().getEntry(indexPosition);
        // block file index is after the int indicating the offset in the index
        // file
        return range.getBuf().getShort(range.getStartOffset() + (Integer.SIZE / 8));
    }
    
    protected static ByteRange getBlockEntry(int indexPosition, BlockReader index) {
        ByteRange range = index.getValues().getEntry(indexPosition);
        // block file index is after the int indicating the offset in the index
        // file
        return range;
    }
    
    protected static int getBlockIndexOffset(ByteBuffer buf, int startOffset) {
        return buf.getInt(startOffset);
    }
    
    protected static short getBlockIndexFileId(ByteBuffer buf, int startOffset) {
        return buf.getShort(startOffset + (Integer.SIZE / 8));
    }
}
