/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.ByteRange;
import org.xtreemfs.foundation.logging.Logging;

public abstract class DiskIndexIteratorBase {
    
    private final DiskIndex                         index;
    
    private final byte[]                            from;
    
    private final byte[]                            to;
    
    private final BlockReader                       blockIndexReader;
    
    private final ByteBuffer[]                      maps;
    
    private final FileChannel[]                     dbFileChannels;
    
    private final int                               blockIndexStart;
    
    private final int                               blockIndexEnd;
    
    private final boolean                           ascending;
    
    private int                                     currentBlockIndex;
    
    private BlockReader                             currentBlock;
    
    protected Iterator<Entry<ByteRange, ByteRange>> currentBlockIterator;
    
    protected DiskIndexIteratorBase(DiskIndex index, BlockReader blockIndexReader, byte[] from, byte[] to,
        boolean ascending, ByteBuffer[] maps, FileChannel[] dbFileChannels) {
        
        this.maps = maps;
        this.dbFileChannels = dbFileChannels;
        this.index = index;
        this.from = from;
        this.to = to;
        this.ascending = ascending;
        
        this.blockIndexReader = blockIndexReader.clone();
        
        // determine the first potential block containing entries with keys in
        // the range
        int tmp = from == null ? 0 : index.getBlockIndexPosition(from, blockIndexReader);
        if (tmp < 0)
            tmp = 0;
        this.blockIndexStart = tmp;
        
        // determine the last potential block containing entries with keys in
        // the range
        tmp = to == null ? blockIndexReader.getNumEntries() - 1 : index.getBlockIndexPosition(to,
            blockIndexReader);
        if (tmp > blockIndexReader.getNumEntries() - 1)
            tmp = blockIndexReader.getNumEntries() - 1;
        this.blockIndexEnd = tmp;
        
        currentBlockIndex = ascending ? blockIndexStart : blockIndexEnd;
        getNextBlockData();
    }
    
    public boolean hasNext() {
        
        while (currentBlockIterator != null) {
            
            if (currentBlockIterator.hasNext())
                return true;
            
            if (ascending)
                currentBlockIndex++;
            else
                currentBlockIndex--;
            
            getNextBlockData();
        }
        
        return false;
    }
    
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public void free() {
        // also check if the buffer has been returned already; this may happen
        // if all elements have been accessed before
        if (currentBlock != null && currentBlock.readBuffer.getRefCount() > 0)
            currentBlock.free();
    }
    
    protected void finalize() throws Throwable {
        free();
        super.finalize();
    }
    
    private void getNextBlockData() {
        
        if (blockIndexStart == -1 && blockIndexEnd == -1)
            return;
        
        // ascending
        if (ascending && currentBlockIndex > blockIndexEnd) {
            currentBlock = null;
            currentBlockIterator = null;
            return;
        }

        // descending
        else if (!ascending && currentBlockIndex < blockIndexStart) {
            currentBlock = null;
            currentBlockIterator = null;
            return;
        }
        
        int startOffset = DiskIndex.getBlockOffset(currentBlockIndex, blockIndexReader);
        // when last block or a single block the offset should be the
        // size of the block
        
        int fileId = DiskIndex.getBlockFileId(currentBlockIndex, blockIndexReader);
        int endOffset;
        if (currentBlockIndex == blockIndexReader.getNumEntries() - 1)
            // the last block in the block index
            endOffset = -1;
        else {
            ByteRange indexPos = DiskIndex.getBlockEntry(currentBlockIndex + 1, blockIndexReader);
            ByteBuffer indexPosBuf = indexPos.getBuf();
            endOffset = DiskIndex.getBlockIndexOffset(indexPosBuf, indexPos.getStartOffset());
            
            // is this the last block of the current block file?
            // then the endBlockOffset should be set to the end of the
            // file
            if (DiskIndex.getBlockIndexFileId(indexPosBuf, indexPos.getStartOffset()) > fileId)
                endOffset = -1;
            
            // endBlockOffset = getBlockOffset(indexPosition + 1,
            // blockIndex);
        }
        
        try {
            currentBlock = maps != null ? index.getBlock(startOffset, endOffset, maps[fileId]) : index
                    .getBlock(startOffset, endOffset, dbFileChannels[fileId]);
        } catch (ClosedByInterruptException exc) {
            Logging.logError(Logging.LEVEL_DEBUG, this, exc);
        } catch (IOException exc) {
            Logging.logError(Logging.LEVEL_ERROR, this, exc);
        }
        
        currentBlockIterator = currentBlock == null ? null : currentBlock.rangeLookup(from == null ? null
            : from, to == null ? null : to, ascending);
    }
    
}
