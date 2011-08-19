/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.CheckpointerInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.dev.ResponseManagerInternal;
import org.xtreemfs.babudb.api.dev.SnapshotManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.foundation.LifeCycleThread;

/**
 * Stub to the {@link BabuDB} API. This is used to decide whether some operation
 * may be executed locally, or should be executed on a remote master.
 * 
 * @author flangner
 * @since 11/03/2010
 */
public class BabuDBProxy implements BabuDBInternal {
    
    private final BabuDBInternal          localBabuDB;
    private final TransactionManagerProxy txnManProxy;
    private final DatabaseManagerInternal dbManProxy;
    private final ReplicationManager      replMan;
    private final ProxyAccessClient       client;
    
    public BabuDBProxy(BabuDBInternal localDB, ReplicationManager replMan, 
            Policy replicationPolicy) {    
        
        assert (localDB != null);
        
        this.localBabuDB = localDB;
        this.replMan = replMan;
        this.txnManProxy = new TransactionManagerProxy(replMan, 
                localDB.getTransactionManager(), replicationPolicy, this);
        DatabaseManagerProxy dbMan = new DatabaseManagerProxy(localDB.getDatabaseManager(), 
                replicationPolicy, replMan, this, txnManProxy, localDB.getResponseManager());
        this.client = replMan.getProxyClient(dbMan);
        this.dbManProxy = dbMan;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getCheckpointer()
     */
    @Override
    public CheckpointerInternal getCheckpointer() {
        return localBabuDB.getCheckpointer();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getDatabaseManager()
     */
    @Override
    public DatabaseManagerInternal getDatabaseManager() {
        return dbManProxy;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getSnapshotManager()
     */
    @Override
    public SnapshotManagerInternal getSnapshotManager() {
        return localBabuDB.getSnapshotManager();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        shutdown(true);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown(boolean)
     */
    @Override
    public void shutdown(boolean graceful) throws BabuDBException {
        try {
            replMan.shutdown();
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        } finally {
            localBabuDB.shutdown(graceful); 
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.BabuDBInternal#getTransactionManager()
     */
    @Override
    public TransactionManagerInternal getTransactionManager() {
        return txnManProxy;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#
     * init(org.xtreemfs.babudb.api.StaticInitialization)
     */
    @Override
    public void init(StaticInitialization staticInit) throws BabuDBException {
        try {
            localBabuDB.init(staticInit);
            localBabuDB.replaceTransactionManager(txnManProxy);
            replMan.init();
        } catch (InterruptedException e) {
            throw new BabuDBException(ErrorCode.INTERRUPTED, e.getMessage());
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#
     * addPluginThread(org.xtreemfs.foundation.LifeCycleThread)
     */
    @Override
    public void addPluginThread(LifeCycleThread plugin) {
        localBabuDB.addPluginThread(plugin);
    }
    
/*
 * unsupported
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#getDBConfigFile()
     */
    @Override
    public DBConfig getDBConfigFile() {
        throw new UnsupportedOperationException("Manually influencing the" +
        		" DBConfig is forbidden by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#getConfig()
     */
    @Override
    public BabuDBConfig getConfig() {
        return localBabuDB.getConfig();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#getWorker(int)
     */
    @Override
    public LSMDBWorker getWorker(int dbId) {
        return localBabuDB.getWorker(dbId);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#getWorkerCount()
     */
    @Override
    public int getWorkerCount() {
        return localBabuDB.getWorkerCount();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#stop()
     */
    @Override
    public void stop() {
        throw new UnsupportedOperationException("Manually stopping the local" +
        	" BabuDB instance is forbidden by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#restart()
     */
    @Override
    public LSN restart() throws BabuDBException {
        throw new UnsupportedOperationException("Manually restarting the local" 
                + " BabuDB instance is forbidden by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.BabuDBInternal#replaceTransactionManager(
     *          org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal)
     */
    @Override
    public void replaceTransactionManager(TransactionManagerInternal txnMan) {
        throw new UnsupportedOperationException("Manually changing the " +
        		"persistence manager of the local BabuDB instance" +
        		" is forbidden by the replication plugin.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        localBabuDB.startupPerformed();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        localBabuDB.shutdownPerformed();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable cause) {
        localBabuDB.crashPerformed(cause);
    }
    
    /**
     * @return the client for proxy requests.
     */
    public ProxyAccessClient getClient() {
        return client;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.BabuDBInternal#getResponseManager()
     */
    @Override
    public ResponseManagerInternal getResponseManager() {
        return localBabuDB.getResponseManager();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getRuntimeState(java.lang.String)
     */
    @Override
    public Object getRuntimeState(String propertyName) {
        
        if (!propertyName.startsWith("replication")) {
            return localBabuDB.getRuntimeState(propertyName);
        } else {
            return replMan.getRuntimeState(propertyName);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getRuntimeState()
     */
    @Override
    public Map<String, Object> getRuntimeState() {
        
        Map<String, Object> info = new HashMap<String, Object>();
        info.putAll(localBabuDB.getRuntimeState());
        info.putAll(replMan.getRuntimeState());
        
        return info;
    }
}
