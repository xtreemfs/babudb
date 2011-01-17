/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.index.overlay;

import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;

public class MultiOverlayBufferTree extends MultiOverlayTree<byte[], byte[]> {
    
    private ByteRangeComparator comp;
    
    public MultiOverlayBufferTree(byte[] markerElement, ByteRangeComparator comp) {
        super(markerElement, comp);
        this.comp = comp;
    }
    
    public ResultSet<byte[], byte[]> prefixLookup(byte[] prefix, boolean includeDeletedEntries,
        boolean ascending) {
        
        byte[][] keyRange = comp.prefixToRange(prefix, ascending);
        assert (keyRange.length == 2);
        
        return rangeLookup(keyRange[0], keyRange[1], includeDeletedEntries, ascending);
    }
    
    public ResultSet<byte[], byte[]> prefixLookup(byte[] prefix, int overlayId,
        boolean includeDeletedEntries, boolean ascending) {
        
        byte[][] keyRange = comp.prefixToRange(prefix, ascending);
        assert (keyRange.length == 2);
        
        return rangeLookup(keyRange[0], keyRange[1], overlayId, includeDeletedEntries, ascending);
    }
    
}
