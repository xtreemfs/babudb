/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;

/**
 * This class provides simple read-only access to all immutable on-disk indices
 * of a BabuDB database.
 * 
 * @author stender
 * 
 */
public class InMemoryView implements BabuDBView {
    
    private DatabaseImpl          db;
    
    private Map<Integer, Integer> snapIDMap;
    
    private SnapshotConfig        snap;
    
    public InMemoryView(BabuDB babuDB, String dbName, SnapshotConfig snap, int[] snapIDs)
        throws BabuDBException {
        
        this.db = (DatabaseImpl) babuDB.getDatabaseManager().getDatabase(dbName);
        this.snap = snap;
        
        snapIDMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < snap.getIndices().length; i++)
            snapIDMap.put(snap.getIndices()[i], snapIDs[i]);
        
    }
    
    @Override
    public byte[] directLookup(int indexId, byte[] key) throws BabuDBException {
        
        Integer snapId = snapIDMap.get(indexId);
        if (snapId == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        return (isCovered(indexId, key) && snap.containsKey(indexId, key)) ? db.directLookup(indexId, snapId,
            key) : null;
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> directPrefixLookup(final int indexId, final byte[] key,
        final boolean ascending) throws BabuDBException {
        
        final Integer snapId = snapIDMap.get(indexId);
        if (snapId == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        return new Iterator<Entry<byte[], byte[]>>() {
            
            private Iterator<Entry<byte[], byte[]>> it;
            
            private Entry<byte[], byte[]>           next;
            
            {
                it = db.directPrefixLookup(indexId, snapId, key, ascending);
                getNextEntry();
            }
            
            @Override
            public boolean hasNext() {
                return next != null;
            }
            
            @Override
            public Entry<byte[], byte[]> next() {
                
                if (next == null)
                    throw new NoSuchElementException();
                
                Entry<byte[], byte[]> tmp = next;
                getNextEntry();
                
                return tmp;
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
            private void getNextEntry() {
                
                while (it.hasNext()) {
                    next = it.next();
                    if (isCovered(indexId, next.getKey()) && snap.containsKey(indexId, next.getKey()))
                        return;
                }
                
                next = null;
            }
            
        };
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> directRangeLookup(final int indexId, final byte[] from,
        final byte[] to, final boolean ascending) throws BabuDBException {
        
        final Integer snapId = snapIDMap.get(indexId);
        if (snapId == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        return new Iterator<Entry<byte[], byte[]>>() {
            
            private Iterator<Entry<byte[], byte[]>> it;
            
            private Entry<byte[], byte[]>           next;
            
            {
                it = db.directRangeLookup(indexId, snapId, from, to, ascending);
                getNextEntry();
            }
            
            @Override
            public boolean hasNext() {
                return next != null;
            }
            
            @Override
            public Entry<byte[], byte[]> next() {
                
                if (next == null)
                    throw new NoSuchElementException();
                
                Entry<byte[], byte[]> tmp = next;
                getNextEntry();
                
                return tmp;
            }
            
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
            private void getNextEntry() {
                
                while (it.hasNext()) {
                    next = it.next();
                    if (isCovered(indexId, next.getKey()) && snap.containsKey(indexId, next.getKey()))
                        return;
                }
                
                next = null;
            }
            
        };
    }
    
    @Override
    public void shutdown() throws BabuDBException {
        // nothing to do
    }
    
    private boolean isCovered(int index, byte[] key) {
        
        if (snap.getPrefixes(index) == null)
            return true;
        
        for (byte[] prefix : snap.getPrefixes(index))
            if (startsWith(key, prefix))
                return true;
        
        return false;
    }
    
    private static boolean startsWith(byte[] key, byte[] prefix) {
        
        if (key.length >= prefix.length) {
            
            for (int i = 0; i < prefix.length; i++)
                if (key[i] != prefix[i])
                    return false;
            
            return true;
        }
        
        return false;
    }
    
}
