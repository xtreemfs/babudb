/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.overlay;

import java.util.Iterator;
import java.util.Map.Entry;

public class MultiOverlayStringTree<V> extends MultiOverlayTree<String, V> {
    
    public MultiOverlayStringTree(V markerElement) {
        super(markerElement);
    }
    
    public Iterator<Entry<String, V>> prefixLookup(String prefix, boolean includeDeletedEntries,
        boolean ascending) {
        return rangeLookup(prefix, getNextPrefix(prefix), includeDeletedEntries, ascending);
    }
    
    public Iterator<Entry<String, V>> prefixLookup(String prefix, int overlayId,
        boolean includeDeletedEntries, boolean ascending) {
        return rangeLookup(prefix, getNextPrefix(prefix), overlayId, includeDeletedEntries, ascending);
    }
    
    private String getNextPrefix(String prefix) {
        
        if (prefix == null)
            return null;
        
        byte[] bytes = prefix.getBytes();
        for (int i = bytes.length - 1; i >= 0; i--) {
            bytes[i]++;
            if (bytes[i] != Byte.MIN_VALUE)
                break;
        }
        
        return new String(bytes);
    }
    
}
