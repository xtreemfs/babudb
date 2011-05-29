/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.database.DatabaseRO;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;

public class Snapshot implements DatabaseRO {
        
    private final BabuDBInternal dbs;
    
    private BabuDBView           view;
    
    public Snapshot(BabuDBView view, BabuDBInternal dbs) {
        this.view = view;
        this.dbs = dbs;
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
    public DatabaseRequestResult<byte[]> lookup(int indexId, byte[] key, Object context) {
        BabuDBRequestResultImpl<byte[]> result = 
            new BabuDBRequestResultImpl<byte[]>(context, dbs.getResponseManager());
        byte[] r;
        try {
            r = view.directLookup(indexId, key);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#prefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> prefixLookup(int indexId, byte[] key,
            Object context) {
        
        BabuDBRequestResultImpl<ResultSet<byte[], byte[]>> result = 
            new BabuDBRequestResultImpl<ResultSet<byte[], byte[]>>(context, 
                    dbs.getResponseManager());
        
        ResultSet<byte[], byte[]> r;
        try {
            r = view.directPrefixLookup(indexId, key, true);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> reversePrefixLookup(int indexId, byte[] key,
            Object context) {
        
        BabuDBRequestResultImpl<ResultSet<byte[], byte[]>> result = 
            new BabuDBRequestResultImpl<ResultSet<byte[], byte[]>>(context, 
                    dbs.getResponseManager());
        ResultSet<byte[], byte[]> r;
        try {
            r = view.directPrefixLookup(indexId, key, false);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }
    
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> rangeLookup(int indexId, byte[] from,
        byte[] to, Object context) {
        
        BabuDBRequestResultImpl<ResultSet<byte[], byte[]>> result = 
            new BabuDBRequestResultImpl<ResultSet<byte[], byte[]>>(context, 
                    dbs.getResponseManager());
        ResultSet<byte[], byte[]> r;
        try {
            r = view.directRangeLookup(indexId, from, to, true);
            result.finished(r);
        } catch (BabuDBException e) {
            result.failed(e);
        }
        
        return result;
    }

    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> reverseRangeLookup(int indexId, byte[] from,
        byte[] to, Object context) {

        BabuDBRequestResultImpl<ResultSet<byte[], byte[]>> result = 
            new BabuDBRequestResultImpl<ResultSet<byte[], byte[]>>(context, 
                    dbs.getResponseManager());
        
        ResultSet<byte[], byte[]> r;
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
    public DatabaseRequestResult<Object> userDefinedLookup(UserDefinedLookup udl, Object context) {
        throw new UnsupportedOperationException();
    }

}
