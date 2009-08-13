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
    
    public InMemoryView(BabuDB babuDB, String dbName, int[] indices, int[] snapIDs) throws BabuDBException {
        
        this.db = (DatabaseImpl) babuDB.getDatabaseManager().getDatabase(dbName);
        
        snapIDMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < indices.length; i++)
            snapIDMap.put(indices[i], snapIDs[i]);
        
    }
    
    @Override
    public byte[] directLookup(int indexId, byte[] key) throws BabuDBException {
        
        Integer snapId = snapIDMap.get(indexId);
        if (snapId == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        return db.directLookup(indexId, snapId, key);
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> directPrefixLookup(int indexId, byte[] key) throws BabuDBException {
        
        Integer snapId = snapIDMap.get(indexId);
        if (snapId == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        
        return db.directPrefixLookup(indexId, snapId, key);
    }
    
    @Override
    public void shutdown() throws BabuDBException {
        // nothing to do
    }
    
}
