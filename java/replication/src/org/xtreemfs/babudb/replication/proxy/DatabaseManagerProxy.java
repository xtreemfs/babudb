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
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;
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
class DatabaseManagerProxy implements DatabaseManagerInternal {

    private final    DatabaseManagerInternal    localDBMan;
    private final    Policy                     replicationPolicy;
    private final    ReplicationManager         replicationManager;
    private final    ProxyAccessClient          client;
    private final    TransactionManagerInternal txnManProxy;

    public DatabaseManagerProxy(DatabaseManagerInternal localDBMan, Policy policy, 
            ReplicationManager replMan, ProxyAccessClient client, 
            TransactionManagerInternal persMan) {
        
        assert (localDBMan != null);
        
        this.txnManProxy = persMan;
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
    public DatabaseInternal getDatabase(String dbName) throws BabuDBException {
        
        InetSocketAddress master = getServerToPerformAt();
        if (master == null) {
            return new DatabaseProxy(localDBMan.getDatabase(dbName), replicationPolicy, this);
        }
        
        try {
            
            int dbId = client.getDatabase(dbName, master).get();
            return new DatabaseProxy(dbName, dbId, replicationPolicy, this);
            
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
        
        try {
            InetSocketAddress master = getServerToPerformAt();
            
            Map<String, DatabaseInternal> r = new HashMap<String, DatabaseInternal>();
            if (master == null) {  
                
                for (Entry<String, DatabaseInternal> e : 
                    localDBMan.getDatabasesInternal().entrySet()) {
                    
                    r.put(e.getKey(), new DatabaseProxy(e.getValue(), replicationPolicy, this));
                }
            } else {
                for (Entry<String, Integer> e : 
                    client.getDatabases(master).get().entrySet()) {
                                    
                    r.put(e.getKey(), new DatabaseProxy(e.getKey(), e.getValue(), replicationPolicy, 
                            this));
                }
            }
            return r; 
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_ERROR, this, e);
            return new HashMap<String, DatabaseInternal>();
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
        return new DatabaseProxy(localDBMan.createDatabase(databaseName, numIndices, comparators), 
                replicationPolicy, this);
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
     * @return the host to perform the request at, or null, if it is permitted to perform the 
     *         request locally.
     * @throws BabuDBException if replication is currently not available.
     */
    private InetSocketAddress getServerToPerformAt() throws BabuDBException {
        InetSocketAddress master;
        try {
            master = replicationManager.getMaster();
        } catch (InterruptedException e) {
            throw new BabuDBException(ErrorCode.INTERRUPTED, 
                "Waiting for a lease holder was interrupted.", e);
        }
        
        if (replicationManager.isItMe(master) || 
            !replicationPolicy.dbModificationIsMasterRestricted()) {
            return null;
        }
        
        return master;
    }

    DatabaseInternal getLocalDatabase(String name) throws BabuDBException {
        return localDBMan.getDatabase(name);
    }
    
    ProxyAccessClient getClient() {
        return client;
    }
    
    TransactionManagerInternal getTransactionManager() {
        return txnManProxy;
    }
    
    ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabaseList()
     */
    @Override
    public Collection<DatabaseInternal> getDatabaseList() {
        return getDatabasesInternal().values();
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
        InetSocketAddress master = getServerToPerformAt();
        if (master == null) {
            return new DatabaseProxy(localDBMan.getDatabase(dbId), replicationPolicy, this);
        }

        try {
            String dbName = client.getDatabase(dbId, master).get();
            return new DatabaseProxy(dbName, dbId, replicationPolicy, this);
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
            if (getServerToPerformAt() == null) {
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
            if (getServerToPerformAt() == null) {
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
            if (getServerToPerformAt() == null) {
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
            if (getServerToPerformAt() == null) {
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
            if (getServerToPerformAt() == null) {
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
            if (getServerToPerformAt() == null) {
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
}
