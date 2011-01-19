/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.proxy;

import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;

/**
 * Stub to redirect Database read-only requests to a remote master if necessary.
 * 
 * @see Policy
 * 
 * @author flangner
 * @since 01/19/2011
 */
class DatabaseProxy implements Database {

    private final ReplicationManager replMan;
    private final Database localDB;
    private final Policy replicationPolicy;
    
    public DatabaseProxy(Database localDatabase, ReplicationManager replMan, 
            Policy replicationPolicy) {
        
        this.localDB = localDatabase;
        this.replMan = replMan;
        this.replicationPolicy = replicationPolicy;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#lookup(int, byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<byte[]> lookup(int indexId, byte[] key, Object context) {
        if (hasPermissionToExecuteLocally()) {
            
            return lookup(indexId, key, context);
        }
        
        return null; // TODO RPC
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#prefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Iterator<Entry<byte[], byte[]>>> prefixLookup(int indexId, byte[] key,
            Object context) {
        
        if (hasPermissionToExecuteLocally()) {
            
            return localDB.prefixLookup(indexId, key, context);
        }
        // TODO RPC
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#reversePrefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Iterator<Entry<byte[], byte[]>>> reversePrefixLookup(int indexId,
            byte[] key, Object context) {
        
        if (hasPermissionToExecuteLocally()) {
            return reversePrefixLookup(indexId, key, context);
        }
        // TODO RPC
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#rangeLookup(int, byte[], byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Iterator<Entry<byte[], byte[]>>> rangeLookup(int indexId, byte[] from,
            byte[] to, Object context) {
        
        if (hasPermissionToExecuteLocally()) {
            return rangeLookup(indexId, from, to, context);
        }
        
        // TODO RPC
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#reverseRangeLookup(int, byte[], byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Iterator<Entry<byte[], byte[]>>> reverseRangeLookup(int indexId,
            byte[] from, byte[] to, Object context) {
        
        if (hasPermissionToExecuteLocally()) {
            return reverseRangeLookup(indexId, from, to, context);
        }
        
        // TODO RPC
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#userDefinedLookup(org.xtreemfs.babudb.api.database.UserDefinedLookup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> userDefinedLookup(UserDefinedLookup udl, Object context) {
        
        if (hasPermissionToExecuteLocally()) {
            return localDB.userDefinedLookup(udl, context);
        }
        // TODO RPC
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        
        if (hasPermissionToExecuteLocally()) {
            localDB.shutdown();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#getName()
     */
    @Override
    public String getName() {
        return localDB.getName();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#createInsertGroup()
     */
    @Override
    public DatabaseInsertGroup createInsertGroup() {
        return localDB.createInsertGroup();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#getComparators()
     */
    @Override
    public ByteRangeComparator[] getComparators() {
        return localDB.getComparators();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#singleInsert(int, byte[], byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> singleInsert(int indexId, byte[] key, byte[] value, Object context) {
        return localDB.singleInsert(indexId, key, value, context);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#insert(org.xtreemfs.babudb.api.database.DatabaseInsertGroup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> insert(DatabaseInsertGroup irg, Object context) {
        return localDB.insert(irg, context);
    }

    private boolean hasPermissionToExecuteLocally () {
        return localDB != null && (!replicationPolicy.lookUpIsMasterRestricted() 
                || replMan.isMaster());
    }
}
