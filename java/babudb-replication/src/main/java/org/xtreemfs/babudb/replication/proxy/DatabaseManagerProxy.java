/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import static org.xtreemfs.babudb.replication.transmission.ErrorCode.mapTransmissionError;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.dev.ResponseManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.babudb.replication.proxy.BabuDBProxy.RequestRerunner;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Stub to redirect Database read-only requests to a remote master if necessary.
 * 
 * @see Policy
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class DatabaseManagerProxy implements DatabaseManagerInternal {

    private final    DatabaseManagerInternal    localDBMan;
    private final    Policy                     replicationPolicy;
    private final    ReplicationManager         replicationManager;
    private final    BabuDBProxy                babuDBProxy;
    private final    TransactionManagerInternal txnManProxy;
    private final    ResponseManagerInternal    responseManager;
    
    public DatabaseManagerProxy(DatabaseManagerInternal localDBMan, Policy policy, 
            ReplicationManager replMan, BabuDBProxy babuDBProxy, 
            TransactionManagerInternal persMan, ResponseManagerInternal respMan) {
        
        assert (localDBMan != null && !(localDBMan instanceof DatabaseManagerProxy));
        
        this.txnManProxy = persMan;
        this.localDBMan = localDBMan;
        this.replicationPolicy = policy;
        this.replicationManager = replMan;
        this.babuDBProxy = babuDBProxy;
        this.responseManager = respMan;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabase(
     *          java.lang.String)
     */
    @Override
    public DatabaseInternal getDatabase(String dbName) throws BabuDBException {
       
        BabuDBException be = null;
        int tries = 0;
        while (ReplicationConfig.PROXY_MAX_RETRIES == 0 || tries++ < ReplicationConfig.PROXY_MAX_RETRIES) {
            
            InetSocketAddress master = getServerToPerformAt(0);
            if (master == null) {
                return new DatabaseProxy(localDBMan.getDatabase(dbName), this);
            }
            try {
                
                int dbId = babuDBProxy.getClient().getDatabase(dbName, master).get();
                return new DatabaseProxy(dbName, dbId, this);
                
            } catch (ErrorCodeException ece) {
                ErrorCode code = mapTransmissionError(ece.getCode());
                be = new BabuDBException(code, ece.getMessage());
                if (code != ErrorCode.REPLICATION_FAILURE) {
                    throw be;
                }
            } catch (Exception e) {
                be = new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
            }
            
            try {
                Thread.sleep(ReplicationConfig.PROXY_RETRY_DELAY);
            } catch (InterruptedException ie) {
                throw new BabuDBException(ErrorCode.INTERRUPTED, ie.getMessage());
            }
        }

        throw be;
    }
    
    // TODO ugly code! redesign!!
    public DatabaseProxy getDatabaseNonblocking(String dbName) throws BabuDBException {
        
        InetSocketAddress master = getServerToPerformAt(-1);
        if (master == null) {
            return new DatabaseProxy(localDBMan.getDatabase(dbName), this);
        }
        
        try {
            
            int dbId = babuDBProxy.getClient().getDatabase(dbName, master).get();
            return new DatabaseProxy(dbName, dbId, this);
            
        } catch (ErrorCodeException ece) {
            throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabasesInternal()
     */
    @Override
    public Map<String, DatabaseInternal> getDatabasesInternal() {
        
        Exception ex = null;
        int tries = 0;
        while (ReplicationConfig.PROXY_MAX_RETRIES == 0 || tries++ < ReplicationConfig.PROXY_MAX_RETRIES) {
            try {
                InetSocketAddress master = getServerToPerformAt(0);
                
                Map<String, DatabaseInternal> r = new HashMap<String, DatabaseInternal>();
                if (master == null) {  
                    
                    for (Entry<String, DatabaseInternal> e : 
                        localDBMan.getDatabasesInternal().entrySet()) {
                        
                        r.put(e.getKey(), new DatabaseProxy(e.getValue(), this));
                    }
                } else {
                    for (Entry<String, Integer> e : 
                        babuDBProxy.getClient().getDatabases(master).get().entrySet()) {
                                        
                        r.put(e.getKey(), new DatabaseProxy(e.getKey(), e.getValue(), this));
                    }
                }
                return r; 
            } catch (Exception exc) {
                ex = exc;
            }
        }
        
        Logging.logError(Logging.LEVEL_ERROR, this, ex);
        return new HashMap<String, DatabaseInternal>();
    }
    
    // TODO ugly code! redesign!!
    public Map<String, DatabaseProxy> getDatabasesInternalNonblocking() {
        
        try {
            InetSocketAddress master = getServerToPerformAt(-1);
            
            Map<String, DatabaseProxy> r = new HashMap<String, DatabaseProxy>();
            if (master == null) {  
                
                for (Entry<String, DatabaseInternal> e : 
                    localDBMan.getDatabasesInternal().entrySet()) {
                    
                    r.put(e.getKey(), new DatabaseProxy(e.getValue(), this));
                }
            } else {
                for (Entry<String, Integer> e : 
                    babuDBProxy.getClient().getDatabases(master).get().entrySet()) {
                                    
                    r.put(e.getKey(), new DatabaseProxy(e.getKey(), e.getValue(), this));
                }
            }
            return r; 
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_ERROR, this, e);
            return new HashMap<String, DatabaseProxy>();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabases()
     */
    @Override
    public Map<String, Database> getDatabases() {
        return new HashMap<String, Database>(getDatabasesInternal());
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(
     *          java.lang.String, int)
     */
    @Override
    public DatabaseInternal createDatabase(String databaseName, int numIndices) 
            throws BabuDBException {
        return createDatabase(databaseName, numIndices, null);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(
     *          java.lang.String, int, 
     *          org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public DatabaseInternal createDatabase(String databaseName, int numIndices, 
            ByteRangeComparator[] comparators) throws BabuDBException {
        
        DatabaseInternal result = localDBMan.createDatabase(databaseName, numIndices, comparators);
        if (result instanceof DatabaseProxy) {
            return result;
        } else {
            return new DatabaseProxy(result, this);
        }
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
     * @param timeout - 0 means infinitly and < 0 non blocking.
     * 
     * @return the host to perform the request at, or null, if it is permitted to perform the 
     *         request locally.
     * @throws BabuDBException if replication is currently not available.
     */
    private InetSocketAddress getServerToPerformAt(int timeout) throws BabuDBException {
        InetSocketAddress master;
        try {
            master = replicationManager.getMaster(timeout);
        } catch (InterruptedException e) {
            throw new BabuDBException(ErrorCode.INTERRUPTED, 
                "Waiting for a lease holder was interrupted.", e);
        }
        
        if (replicationManager.isItMe(master) || 
            !replicationPolicy.dbModificationIsMasterRestricted()) {
            return null;
        }
        
        if (replicationManager.redirectIsVisible()) {
            throw new BabuDBException(ErrorCode.REDIRECT, master.toString());
        }
        return master;
    }

    DatabaseInternal getLocalDatabase(String name) throws BabuDBException {
        return localDBMan.getDatabase(name);
    }
    
    ProxyAccessClient getClient() {
        return babuDBProxy.getClient();
    }
    
    TransactionManagerInternal getTransactionManager() {
        return txnManProxy;
    }
    
    ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    RequestRerunner getRequestRerunner() {
        return babuDBProxy.getRequestRerunner();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabaseList()
     */
    @Override
    public Collection<DatabaseInternal> getDatabaseList() {
        return getDatabasesInternal().values();
    }
    
    /**
     * @return a list of local databases.
     */
    public Collection<DatabaseInternal> getLocalDatabaseList() {
        return localDBMan.getDatabasesInternal().values();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDBModificationLock()
     */
    @Override
    public Object getDBModificationLock() {
        return localDBMan.getDBModificationLock();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#reset()
     */
    @Override
    public void reset() throws BabuDBException {
        localDBMan.reset();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        localDBMan.shutdown();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabase(int)
     */
    @Override
    public DatabaseInternal getDatabase(int dbId) throws BabuDBException {
        
        BabuDBException be = null;
        int tries = 0;
        while (ReplicationConfig.PROXY_MAX_RETRIES == 0 || tries++ < ReplicationConfig.PROXY_MAX_RETRIES) {
            InetSocketAddress master = getServerToPerformAt(0);
            if (master == null) {
                return new DatabaseProxy(localDBMan.getDatabase(dbId), this);
            }
    
            try {
                String dbName = babuDBProxy.getClient().getDatabase(dbId, master).get();
                return new DatabaseProxy(dbName, dbId, this);
            } catch (ErrorCodeException ece) {
                ErrorCode code = mapTransmissionError(ece.getCode());
                be = new BabuDBException(code, ece.getMessage());
                if (code != ErrorCode.REPLICATION_FAILURE) {
                    throw be;
                }
            } catch (Exception e) {
                be = new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
            }
        }
        
        throw be;
    }
    
    // TODO ugly code! redesign!!
    public DatabaseProxy getDatabaseNonblocking(int dbId) throws BabuDBException {
        InetSocketAddress master = getServerToPerformAt(-1);
        if (master == null) {
            return new DatabaseProxy(localDBMan.getDatabase(dbId), this);
        }

        try {
            String dbName = babuDBProxy.getClient().getDatabase(dbId, master).get();
            return new DatabaseProxy(dbName, dbId, this);
        } catch (ErrorCodeException ece) {
            throw new BabuDBException(mapTransmissionError(ece.getCode()),ece.getMessage());
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        }
    }
    
/*
 * TODO is it really necessary to not support the internal mechanisms of the DatabaseManager?
 */

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getNextDBId()
     */
    @Override
    public int getNextDBId() {
        try {
            if (getServerToPerformAt(0) == null) {
                return localDBMan.getNextDBId();
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Manually influencing the DatabaseMangager of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#setNextDBId(int)
     */
    @Override
    public void setNextDBId(int id) {
        try {
            if (getServerToPerformAt(0) == null) {
                localDBMan.setNextDBId(id);
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Manually influencing the DatabaseMangager of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getComparatorInstances()
     */
    @Override
    public Map<String, ByteRangeComparator> getComparatorInstances() {
        try {
            if (getServerToPerformAt(0) == null) {
                return localDBMan.getComparatorInstances();
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Manually influencing the DatabaseMangager of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#putDatabase(org.xtreemfs.babudb.api.dev.DatabaseInternal)
     */
    @Override
    public void putDatabase(DatabaseInternal database) {
        try {
            if (getServerToPerformAt(0) == null) {
                localDBMan.putDatabase(database);
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Manually influencing the DatabaseMangager of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getAllDatabaseIds()
     */
    @Override
    public Set<Integer> getAllDatabaseIds() {
        try {
            if (getServerToPerformAt(0) == null) {
                return localDBMan.getAllDatabaseIds();
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Manually influencing the DatabaseMangager of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#removeDatabaseById(int)
     */
    @Override
    public void removeDatabaseById(int id) {
        try {
            if (getServerToPerformAt(0) == null) {
                localDBMan.removeDatabaseById(id);
            }
        } catch (BabuDBException be) {
            /* ignored */
        }
        throw new UnsupportedOperationException("Manually influencing the DatabaseMangager of a " 
                + "'not master' server is not supported by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#executeTransaction(org.xtreemfs.babudb.api.transaction.Transaction)
     */
    @Override
    public void executeTransaction(Transaction txn) throws BabuDBException {
        localDBMan.executeTransaction(txn);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#addTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void addTransactionListener(TransactionListener listener) {
        localDBMan.addTransactionListener(listener);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#removeTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void removeTransactionListener(TransactionListener listener) {
        localDBMan.removeTransactionListener(listener);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#createTransaction()
     */
    @Override
    public TransactionInternal createTransaction() {
        return localDBMan.createTransaction();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#executeTransaction(org.xtreemfs.babudb.api.dev.transaction.TransactionInternal)
     */
    @Override
    public void executeTransaction(TransactionInternal txn) throws BabuDBException {
        localDBMan.executeTransaction(txn);
    }
    
    public Policy getReplicationPolicy() {
        return replicationPolicy;
    }
    
    public ResponseManagerInternal getResponseManager() {
        return responseManager;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getRuntimeState(java.lang.String)
     */
    @Override
    public Object getRuntimeState(String property) {
        return localDBMan.getRuntimeState(property);
    }
    
    @Override
    public Map<String, Object> getRuntimeState() {
        return localDBMan.getRuntimeState();
    }
}
