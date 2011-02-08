/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.Checkpointer;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.SnapshotManager;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RemoteAccessClient;
import org.xtreemfs.babudb.replication.ReplicationManager;
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
    
    private final BabuDBInternal         localBabuDB;
    private final PersistenceManager     persManProxy;
    private final DatabaseManager        dbManProxy;
    private final ReplicationManager     replMan;
    
    public BabuDBProxy(BabuDBInternal localDB, ReplicationManager replMan, 
            Policy replicationPolicy, RemoteAccessClient client) {    
        
        assert (localDB != null);
        
        this.localBabuDB = localDB;
        this.replMan = replMan;
        this.persManProxy = new PersistenceManagerProxy(replMan, 
                localDB.getPersistenceManager(), replicationPolicy, client);
        this.localBabuDB.replacePersistenceManager(persManProxy);
        this.dbManProxy = new DatabaseManagerProxy(localDB.getDatabaseManager(), 
                replicationPolicy, replMan, client, persManProxy);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getCheckpointer()
     */
    @Override
    public Checkpointer getCheckpointer() {
        return localBabuDB.getCheckpointer();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getDatabaseManager()
     */
    @Override
    public DatabaseManager getDatabaseManager() {
        return dbManProxy;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getSnapshotManager()
     */
    @Override
    public SnapshotManager getSnapshotManager() {
        return localBabuDB.getSnapshotManager();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        try {
            replMan.shutdown();
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                                      e.getMessage());
        } finally {
            localBabuDB.shutdown(); 
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#getPersistenceManager()
     */
    @Override
    public PersistenceManager getPersistenceManager() {
        return persManProxy;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#
     * init(org.xtreemfs.babudb.api.StaticInitialization)
     */
    @Override
    public void init(StaticInitialization staticInit) throws BabuDBException {
        this.localBabuDB.init(staticInit);
        this.replMan.initialize();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#
     * addPluginThread(org.xtreemfs.foundation.LifeCycleThread)
     */
    @Override
    public void addPluginThread(LifeCycleThread plugin) {
        this.localBabuDB.addPluginThread(plugin);
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
     * @see org.xtreemfs.babudb.BabuDBInternal#replacePersistenceManager(
     *          org.xtreemfs.babudb.api.PersistenceManager)
     */
    @Override
    public void replacePersistenceManager(PersistenceManager perMan) {
        throw new UnsupportedOperationException("Manually changing the " +
        		"persistence manager of the local BabuDB instance" +
        		" is forbidden by the replication plugin.");
    }
}
