/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.replication.RemoteAccessClient;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.babudb.replication.transmission.PBRPCClientAdapter.ErrorCodeException;

/**
 * Stub to redirect Database read-only requests to a remote master if necessary.
 * 
 * @see Policy
 * 
 * @author flangner
 * @since 01/19/2011
 */
class DatabaseManagerProxy implements DatabaseManager {

    private final    DatabaseManager    localDBMan;
    private final    Policy             replicationPolicy;
    private final    ReplicationManager replicationManager;
    private final    RemoteAccessClient client;
    private final    PersistenceManager persManProxy;

    public DatabaseManagerProxy(DatabaseManager localDBMan, Policy policy, 
            ReplicationManager replMan, RemoteAccessClient client, PersistenceManager persMan) {
        
        assert (localDBMan != null);
        
        this.persManProxy = persMan;
        this.localDBMan = localDBMan;
        this.replicationPolicy = policy;
        this.replicationManager = replMan;
        this.client = client;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabase(
     *          java.lang.String)
     */
    @Override
    public Database getDatabase(String dbName) throws BabuDBException {
        
        InetSocketAddress master = getServerToPerformAt();
        if (master == null) {
            return new DatabaseProxy(localDBMan.getDatabase(dbName), replicationPolicy, this);
        }
        
        try {
            
            int dbId = client.getDatabase(dbName, master).get();
            return new DatabaseProxy(dbName, dbId, replicationPolicy, this);
            
        } catch (ErrorCodeException e) {
            
            if (org.xtreemfs.babudb.replication.transmission.ErrorCode.DB_UNAVAILABLE == 
                    e.getCode()) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, e.getMessage());
            }
            
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabases()
     */
    @Override
    public Map<String, Database> getDatabases() throws BabuDBException {
        
        InetSocketAddress master = getServerToPerformAt();
        if (master == null) {  
            return localDBMan.getDatabases();
        }
        
        try {
            Map<String, Database> r = new HashMap<String, Database>();
            for (Entry<String, Integer> e : 
                client.getDatabases(master).get().entrySet()) {
                                
                r.put(e.getKey(), new DatabaseProxy(e.getKey(), e.getValue(), replicationPolicy, 
                        this));
            }
            return r; 
        } catch (Exception e) {
            
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        } 
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(
     *          java.lang.String, int)
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices) 
            throws BabuDBException {
        return localDBMan.createDatabase(databaseName, numIndices);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(
     *          java.lang.String, int, 
     *          org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices, 
            ByteRangeComparator[] comparators) throws BabuDBException {
        return localDBMan.createDatabase(databaseName, numIndices, comparators);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#deleteDatabase(
     *          java.lang.String)
     */
    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {
        localDBMan.deleteDatabase(databaseName);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#copyDatabase(
     *          java.lang.String, java.lang.String)
     */
    @Override
    public void copyDatabase(String sourceDB, String destDB) 
            throws BabuDBException {
        localDBMan.copyDatabase(sourceDB, destDB);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#dumpAllDatabases(
     *          java.lang.String)
     */
    @Override
    public void dumpAllDatabases(String destPath) throws BabuDBException, 
            IOException, InterruptedException {
        localDBMan.dumpAllDatabases(destPath);
    }
    
    
    /**
     * @param type - of the request.
     * 
     * @return the host to perform the request at, or null, if it is permitted to perform the 
     *         request locally.
     * @throws BabuDBException if replication is currently not available.
     */
    private InetSocketAddress getServerToPerformAt() throws BabuDBException {
        InetSocketAddress master = replicationManager.getMaster();
        
        if (master == null) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                    "A majority of servers is currently not available.");
        }
        
        if (replicationManager.isItMe(master) || 
            !replicationPolicy.dbModificationIsMasterRestricted()) {
            return null;
        }
        
        return master;
    }

    Database getLocalDatabase(String name) throws BabuDBException {
        return localDBMan.getDatabase(name);
    }
    
    RemoteAccessClient getClient() {
        return client;
    }
    
    PersistenceManager getPersistenceManager() {
        return persManProxy;
    }
    
    ReplicationManager getReplicationManager() {
        return replicationManager;
    }
}
