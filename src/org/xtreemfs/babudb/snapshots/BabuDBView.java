/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.exception.BabuDBException;

public interface BabuDBView {
    
    public byte[] directLookup(int indexId, byte[] key) throws BabuDBException;
    
    public Iterator<Entry<byte[], byte[]>> directPrefixLookup(int indexId, byte[] key, boolean ascending)
        throws BabuDBException;
    
    public Iterator<Entry<byte[], byte[]>> directRangeLookup(int indexId, byte[] from, byte[] to,
        boolean ascending) throws BabuDBException;
    
    public void shutdown() throws BabuDBException;
    
}
