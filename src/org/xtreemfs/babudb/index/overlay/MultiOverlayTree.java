/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.overlay;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.xtreemfs.babudb.index.OverlayMergeIterator;

/**
 * A layered in-memory tree structure.
 * 
 * @author stender
 * 
 */
public class MultiOverlayTree<K, V> {
    
    class OverlayTreeList {
        
        public TreeMap<K, V>   tree;
        
        public OverlayTreeList next;
        
        public OverlayTreeList(TreeMap<K, V> tree, OverlayTreeList next) {
            this.tree = tree;
            this.next = next;
        }
    }
    
    /**
     * value that marks an entry as deleted
     */
    private final V                       nullValue;
    
    /**
     * Comparator for keys
     */
    private Comparator<K>                 comparator;
    
    /**
     * the ID of the current overlay
     */
    private int                           overlayId;
    
    /**
     * overlay ID -> sublist of overlay trees
     */
    private Map<Integer, OverlayTreeList> overlayMap;
    
    /**
     * the list of overlay trees
     */
    private OverlayTreeList               treeList;
    
    /**
     * Creates a new multi-overlay tree. This call is equivalent to
     * <code>MultiOverlayTree(markerElement, null)</code>.
     * 
     * @param nullValue
     *            A value that will never be inserted in the tree. This value
     *            will be used to mark entries as deleted.
     */
    public MultiOverlayTree(V nullValue) {
        this(nullValue, null);
    }
    
    /**
     * Creates a new multi-overlay tree.
     * 
     * @param nullValue
     *            A value that will never be inserted in the tree. This value
     *            will be used to mark entries as deleted.
     * @param comparator
     *            The comparator for the keys. If a <code>null</code> comparator
     *            is provided, the natural ordering of the keys will be used if
     *            defined.
     */
    public MultiOverlayTree(V nullValue, Comparator<K> comparator) {
        
        if (comparator == null) {
            this.comparator = new Comparator<K>() {
                public int compare(K o1, K o2) {
                    return ((Comparable<K>) o1).compareTo(o2);
                }
            };
            
        } else
            this.comparator = comparator;
        
        treeList = new OverlayTreeList(new TreeMap<K, V>(comparator), null);
        overlayMap = new HashMap<Integer, OverlayTreeList>();
        
        this.nullValue = nullValue;
    }
    
    /**
     * Adds a new overlay to the tree. The new overlay becomes writable.
     * 
     * @return the ID of the previous overlay
     */
    public int newOverlay() {
        overlayMap.put(overlayId, treeList);
        treeList = new OverlayTreeList(new TreeMap<K, V>(comparator), treeList);
        return overlayId++;
    }
    
    /**
     * Destroys any read-only overlay trees, such that only the current
     * read-write tree remains.
     */
    public void cleanup() {
        overlayMap.clear();
        treeList.next = null;
        overlayId = 0;
    }
    
    /**
     * Inserts a key-value pair in the LSM tree. If the value is
     * <code>null</code>, the key will be removed.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void insert(K key, V value) {
        
        // delete ...
        if (value == null)
            treeList.tree.put(key, nullValue);
        
        // insert ...
        else
            treeList.tree.put(key, value);
    }
    
    /**
     * Retrieves the value for the given key in the current overlay.
     * 
     * @param key
     *            the key
     * @return the value associated with the key
     */
    public V lookup(K key) {
        return lookup(key, treeList);
    }
    
    /**
     * Retrives the value for the given key in the given overlay.
     * 
     * @param key
     *            the key
     * @param overlayId
     *            the overlay ID
     * @return the value associated with the key in the overlay associated with
     *         the overlay ID
     */
    public V lookup(K key, int overlayId) {
        return lookup(key, overlayMap.get(overlayId));
    }
    
    /**
     * Returns an iterator with all values assocaited with keys between
     * <code>from</code> (inclusively) and <code>to</code> (exclusively).
     * 
     * @param from
     *            the first key (inclusively); if <code>null</code>, the first
     *            key in the map will be used (inclusively)
     * @param to
     *            the last key (exclusively); if <code>null</code>, the last key
     *            in the map will be used (inclusively)
     * @param includeDeletedEntries
     *            If <code>true</code>, entries that have been marked as deleted
     *            will be included in the iterator. The value of such entries
     *            will be the <code>nullValue</code> specified in the
     *            constructor method.
     * @return an iterator with values
     */
    public Iterator<Entry<K, V>> rangeLookup(K from, K to, boolean includeDeletedEntries) {
        return rangeLookup(from, to, treeList, includeDeletedEntries);
    }
    
    /**
     * Returns an iterator with all key-value pairs assocaited with keys between
     * <code>from</code> (inclusively) and <code>to</code> (exclusively) in the
     * given overlay tree.
     * 
     * @param from
     *            the first key (inclusively); if <code>null</code>, the first
     *            key in the map will be used
     * @param to
     *            the last key (exclusively); if <code>null</code>, the last key
     *            in the map will be used (inclusively)
     * @param overlayId
     *            the ID of the overlay
     * @param includeDeletedEntries
     *            If <code>true</code>, entries that have been marked as deleted
     *            will be included in the iterator. The value of such entries
     *            will be the <code>nullValue</code> specified in the
     *            constructor method.
     * @return an iterator with key-value pairs
     */
    public Iterator<Entry<K, V>> rangeLookup(K from, K to, int overlayId,
        boolean includeDeletedEntries) {
        return rangeLookup(from, to, overlayMap.get(overlayId), includeDeletedEntries);
    }
    
    /**
     * Returns the first (smallest) key stored in the overlay tree.
     * 
     * @return the last key in the overlay tree
     */
    public K firstKey() {
        
        OverlayTreeList list = treeList;
        
        K firstKey = null;
        for (; list != null; list = list.next) {
            
            Entry<K, V> firstEntry = list.tree.firstEntry();
            for (;;) {
                if (firstEntry == null) {
                    break;
                } else if (firstEntry.getValue() != nullValue) {
                    if (firstKey == null || comparator.compare(firstEntry.getKey(), firstKey) < 0)
                        firstKey = firstEntry.getKey();
                    break;
                } else
                    firstEntry = list.tree.higherEntry(firstEntry.getKey());
            }
        }
        
        return firstKey;
    }
    
    /**
     * Returns the last (largest) key stored in the overlay tree.
     * 
     * @return the last key in the overlay tree
     */
    public K lastKey() {
        
        OverlayTreeList list = treeList;
        
        K lastKey = null;
        for (; list != null; list = list.next) {
            
            Entry<K, V> lastEntry = list.tree.lastEntry();
            for (;;) {
                if (lastEntry == null) {
                    break;
                } else if (lastEntry.getValue() != nullValue) {
                    if (lastKey == null || comparator.compare(lastEntry.getKey(), lastKey) > 0)
                        lastKey = lastEntry.getKey();
                    break;
                } else
                    lastEntry = list.tree.lowerEntry(lastEntry.getKey());
            }
        }
        
        return lastKey;
    }
    
    private V lookup(K key, OverlayTreeList list) {
        
        for (; list != null; list = list.next) {
            
            V value = list.tree.get(key);
                        
            if (value != null)
                return value;
        }
        
        return null;
    }
    
    private Iterator<Entry<K, V>> rangeLookup(K from, K to, OverlayTreeList treeList,
        boolean includeDeletedEntries) {
        
        // initialize a final list w/ submap iterators of all overlays
        final List<Iterator<Entry<K, V>>> itList = new ArrayList<Iterator<Entry<K, V>>>();
        for (OverlayTreeList list = treeList; list != null; list = list.next) {
            if (from != null && to != null)
                itList.add(list.tree.subMap(from, to).entrySet().iterator());
            else if (from == null && to == null)
                itList.add(list.tree.entrySet().iterator());
            else if (from != null && to == null)
                itList.add(list.tree.tailMap(from).entrySet().iterator());
            else
                itList.add(list.tree.headMap(to).entrySet().iterator());
        }
        
        return new OverlayMergeIterator<K, V>(itList, comparator, includeDeletedEntries ? null
            : nullValue);
    }
}
