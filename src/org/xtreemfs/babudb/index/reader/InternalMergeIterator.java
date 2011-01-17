/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.reader;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.ByteRange;

/**
 * Merges an iterator for an in-memory overlay with an iterator for an on-disk
 * index. <br/>
 * 
 * The iterator either returns a byte array or a <code>ByteRange</code> object,
 * depending on whether the current element is part of the overlay trees or the
 * on-disk index. The returned keys and values are direct references to the
 * internally used key-value pairs and should hence not be modified.
 * 
 * @author stenjan
 * 
 */
public class InternalMergeIterator implements ResultSet<Object, Object> {
    
    private Iterator<Entry<byte[], byte[]>> overlayIterator;
    
    private InternalDiskIndexIterator       diskIndexIterator;
    
    private Entry<byte[], byte[]>           nextOverlayEntry;
    
    private Entry<ByteRange, ByteRange>     nextDiskIndexEntry;
    
    private Entry<Object, Object>           nextEntry;
    
    private ByteRangeComparator             comp;
    
    private byte[]                          nullValue;
    
    private boolean                         ascending;
    
    public InternalMergeIterator(Iterator<Entry<byte[], byte[]>> overlayIterator,
        InternalDiskIndexIterator diskIndexIterator, ByteRangeComparator comp, byte[] nullValue,
        boolean ascending) {
        
        assert (overlayIterator != null);
        
        this.overlayIterator = overlayIterator;
        this.diskIndexIterator = diskIndexIterator;
        this.comp = comp;
        this.nullValue = nullValue;
        this.ascending = ascending;
        
        nextElement();
    }
    
    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }
    
    @Override
    public Entry<Object, Object> next() {
        
        if (nextEntry == null)
            throw new NoSuchElementException();
        
        Entry<Object, Object> tmp = nextEntry;
        
        nextElement();
        return tmp;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public void free() {
        if (diskIndexIterator != null)
            diskIndexIterator.free();
    }
    
    private void nextElement() {
        
        // find the smallest element in the 'rightmost' tree
        for (;;) {
            
            // find the next element in the overlay
            if (nextOverlayEntry == null && overlayIterator.hasNext())
                nextOverlayEntry = overlayIterator.next();
            
            // find the next element in the disk index
            if (nextDiskIndexEntry == null && diskIndexIterator != null && diskIndexIterator.hasNext())
                nextDiskIndexEntry = diskIndexIterator.next();
            
            // if the next overlay key is equal to the next disk index key,
            // shift disk index element
            if (nextOverlayEntry != null && nextDiskIndexEntry != null
                && comp.compare(nextDiskIndexEntry.getKey(), nextOverlayEntry.getKey()) == 0) {
                
                if (diskIndexIterator.hasNext())
                    nextDiskIndexEntry = diskIndexIterator.next();
                else
                    nextDiskIndexEntry = null;
            }
            
            // if no more element exists, set 'next' to 'empty' and return
            if (nextDiskIndexEntry == null && nextOverlayEntry == null) {
                nextEntry = null;
                return;
            }
            
            // otherwise, choose the element with the smallest or largest key,
            // depending on the iteration order
            if (ascending) {
                
                if (nextDiskIndexEntry == null) {
                    nextEntry = InternalBufferUtil.cast(nextOverlayEntry);
                    nextOverlayEntry = null;
                }

                else if (nextOverlayEntry == null) {
                    nextEntry = InternalBufferUtil.cast(nextDiskIndexEntry);
                    nextDiskIndexEntry = null;
                }

                else if (comp.compare(nextDiskIndexEntry.getKey(), nextOverlayEntry.getKey()) < 0) {
                    nextEntry = InternalBufferUtil.cast(nextDiskIndexEntry);
                    nextDiskIndexEntry = null;
                }

                else {
                    nextEntry = InternalBufferUtil.cast(nextOverlayEntry);
                    nextOverlayEntry = null;
                }
                
            } else {
                
                if (nextDiskIndexEntry == null) {
                    nextEntry = InternalBufferUtil.cast(nextOverlayEntry);
                    nextOverlayEntry = null;
                }

                else if (nextOverlayEntry == null) {
                    nextEntry = InternalBufferUtil.cast(nextDiskIndexEntry);
                    nextDiskIndexEntry = null;
                }

                else if (comp.compare(nextDiskIndexEntry.getKey(), nextOverlayEntry.getKey()) > 0) {
                    nextEntry = InternalBufferUtil.cast(nextDiskIndexEntry);
                    nextDiskIndexEntry = null;
                }

                else {
                    InternalBufferUtil.cast(nextOverlayEntry);
                    nextOverlayEntry = null;
                }
                
            }
            
            assert (nextEntry != null);
            
            // if no tombstone value was defined or the next entry's value is
            // not a tombstone value, return; otherwise, restart
            if (nullValue == null || nextEntry.getValue() != nullValue)
                return;
        }
        
    }
    
}
