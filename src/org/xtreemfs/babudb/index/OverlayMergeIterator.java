/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

/**
 * An iterator that merges a list of overlay trees. If a key occurs in multiple
 * trees, the value associated with the key in the first tree has the highest
 * priority, the one in the second tree the second highest priority, and so on.
 * The iterator will never return more than one value for each key.
 * 
 * @author stender
 * 
 * @param <K>
 *            the key type
 * @param <V>
 *            the value type
 */
public class OverlayMergeIterator<K, V> implements Iterator<Entry<K, V>> {
    
    /**
     * the next element to return
     */
    private Entry<K, V>                 nextElement;
    
    /**
     * a list of potentially next elements
     */
    private Entry<K, V>[]               nextElements;
    
    /**
     * a list of all iterators to merge
     */
    private List<Iterator<Entry<K, V>>> itList;
    
    private Comparator<K>               comp;
    
    private V                           nullValue;
    
    private boolean                     ascending;
    
    public OverlayMergeIterator(List<Iterator<Entry<K, V>>> itList, Comparator<K> comp, V nullValue,
        boolean ascending) {
        
        this.itList = itList;
        this.comp = comp;
        this.nullValue = nullValue;
        this.ascending = ascending;
        
        nextElements = new Entry[itList.size()];
        for (int i = 0; i < nextElements.length; i++)
            nextElements[i] = itList.get(i).hasNext() ? itList.get(i).next() : null;
        
        nextElement = getNextElement();
    }
    
    @Override
    public boolean hasNext() {
        return nextElement != null;
    }
    
    @Override
    public Entry<K, V> next() {
        
        if (nextElement == null)
            throw new NoSuchElementException();
        
        Entry<K, V> element = nextElement;
        nextElement = getNextElement();
        return element;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    private Entry<K, V> getNextElement() {
        
        // find the smallest element in the 'rightmost' tree
        for (;;) {
            
            int smallest = 0;
            for (int i = 1; i < nextElements.length; i++) {
                
                if (nextElements[i] == null)
                    continue;
                
                // if a smaller element was found, next element is the
                // smallest
                if (nextElements[smallest] == null
                    || (ascending && comp.compare(nextElements[i].getKey(), nextElements[smallest].getKey()) < 0)
                    || (!ascending && comp.compare(nextElements[i].getKey(), nextElements[smallest].getKey()) > 0))
                    smallest = i;
                
                // if the smallest element is equal to the current one, remove
                // the current one
                else if (comp.compare(nextElements[i].getKey(), nextElements[smallest].getKey()) == 0) {
                    Iterator<Entry<K, V>> it = itList.get(i);
                    nextElements[i] = it.hasNext() ? it.next() : null;
                }
            }
            
            Entry<K, V> entry = nextElements[smallest];
            Iterator<Entry<K, V>> it = itList.get(smallest);
            nextElements[smallest] = it.hasNext() ? it.next() : null;
            
            if (entry == null)
                return null;
            
            if (nullValue == null || entry.getValue() != nullValue)
                return entry;
        }
    }
}
