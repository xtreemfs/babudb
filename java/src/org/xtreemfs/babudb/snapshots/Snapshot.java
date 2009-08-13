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

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.UserDefinedLookup;
import org.xtreemfs.babudb.lsmdb.DatabaseRO;

public class Snapshot implements DatabaseRO {
        
    private BabuDBView view;
    
    public Snapshot(BabuDBView view) {
        this.view = view;
    }
    
    public synchronized BabuDBView getView() {
        return view;
    }
    
    public synchronized void setView(BabuDBView view) {
        this.view = view;
    }
    
    @Override
    public void asyncLookup(int indexId, byte[] key, BabuDBRequestListener listener, Object context)
        throws BabuDBException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void asyncPrefixLookup(int indexId, byte[] key, BabuDBRequestListener listener, Object context)
        throws BabuDBException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void asyncUserDefinedLookup(BabuDBRequestListener listener, UserDefinedLookup udl, Object context)
        throws BabuDBException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public byte[] directLookup(int indexId, byte[] key) throws BabuDBException {
        return view.directLookup(indexId, key);
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> directPrefixLookup(int indexId, byte[] key) throws BabuDBException {
        return view.directPrefixLookup(indexId, key);
    }
    
    @Override
    public void shutdown() throws BabuDBException {
        view.shutdown();
    }
    
    @Override
    public byte[] syncLookup(int indexId, byte[] key) throws BabuDBException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterator<Entry<byte[], byte[]>> syncPrefixLookup(int indexId, byte[] key)
        throws BabuDBException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Object syncUserDefinedLookup(UserDefinedLookup udl) throws BabuDBException {
        throw new UnsupportedOperationException();
    }
    
}
