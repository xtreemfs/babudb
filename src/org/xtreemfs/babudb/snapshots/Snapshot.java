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

import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.BabuDBRequestResult;
import org.xtreemfs.babudb.UserDefinedLookup;
import org.xtreemfs.babudb.api.exceptions.BabuDBException;
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
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        view.shutdown();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#lookup(int, byte[], java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<byte[]> lookup(int indexId, byte[] key, Object context) {
        BabuDBRequest<byte[]> result = new BabuDBRequest<byte[]>(context);
        byte[] r;
        try {
            r = view.directLookup(indexId, key);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#prefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Iterator<Entry<byte[], byte[]>>> prefixLookup(int indexId, byte[] key,
            Object context) {
        
        BabuDBRequest<Iterator<Entry<byte[], byte[]>>> result = 
            new BabuDBRequest<Iterator<Entry<byte[], byte[]>>>(context);
        Iterator<Entry<byte[], byte[]>> r;
        try {
            r = view.directPrefixLookup(indexId, key, true);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    @Override
    public BabuDBRequestResult<Iterator<Entry<byte[], byte[]>>> reversePrefixLookup(int indexId, byte[] key,
            Object context) {
        
        BabuDBRequest<Iterator<Entry<byte[], byte[]>>> result = 
            new BabuDBRequest<Iterator<Entry<byte[], byte[]>>>(context);
        Iterator<Entry<byte[], byte[]>> r;
        try {
            r = view.directPrefixLookup(indexId, key, false);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }
    
    @Override
    public BabuDBRequestResult<Iterator<Entry<byte[], byte[]>>> rangeLookup(int indexId, byte[] from,
        byte[] to, Object context) {
        
        BabuDBRequest<Iterator<Entry<byte[], byte[]>>> result = 
            new BabuDBRequest<Iterator<Entry<byte[], byte[]>>>(context);
        Iterator<Entry<byte[], byte[]>> r;
        try {
            r = view.directRangeLookup(indexId, from, to, true);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    @Override
    public BabuDBRequestResult<Iterator<Entry<byte[], byte[]>>> reverseRangeLookup(int indexId, byte[] from,
        byte[] to, Object context) {

        BabuDBRequest<Iterator<Entry<byte[], byte[]>>> result = 
            new BabuDBRequest<Iterator<Entry<byte[], byte[]>>>(context);
        Iterator<Entry<byte[], byte[]>> r;
        try {
            r = view.directRangeLookup(indexId, from, to, false);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#userDefinedLookup(org.xtreemfs.babudb.UserDefinedLookup, java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Object> userDefinedLookup(UserDefinedLookup udl, Object context) {
        throw new UnsupportedOperationException();
    }

}
