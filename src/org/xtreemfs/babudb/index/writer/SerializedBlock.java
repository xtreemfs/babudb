/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.writer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A serialized representation of a block.
 * 
 * @author stenjan
 * 
 */
public class SerializedBlock {
    
    private List<List<Object>> multiList;
    
    private int                size;
    
    public SerializedBlock() {
        multiList = new LinkedList<List<Object>>();
    }
    
    public void addBuffers(int size, List<Object>... bufferList) {
        for (List<Object> l : bufferList)
            multiList.add(l);
        this.size += size;
    }
    
    public int size() {
        return size;
    }
    
    public Iterator<Object> iterator() {
        
        return new Iterator<Object>() {
            
            private Iterator<Object>       currentIterator;
            
            private Iterator<List<Object>> multiListIterator;
            
            {
                multiListIterator = multiList.iterator();
                currentIterator = multiListIterator.hasNext() ? multiListIterator.next().iterator() : null;
            }
            
            @Override
            public boolean hasNext() {
                return currentIterator != null && currentIterator.hasNext();
            }
            
            @Override
            public Object next() {
                
                Object next = currentIterator.next();
                if (!currentIterator.hasNext())
                    currentIterator = multiListIterator.hasNext() ? multiListIterator.next().iterator()
                        : null;
                
                return next;
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        
    }
    
}
