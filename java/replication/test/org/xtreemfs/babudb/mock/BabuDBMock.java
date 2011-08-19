/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.CheckpointerInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.dev.ResponseManagerInternal;
import org.xtreemfs.babudb.api.dev.SnapshotManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;

/**
 * @author flangner
 * @since 02/21/2011
 */
public class BabuDBMock implements BabuDBInternal {

    private final String                        name;
    private final BabuDBConfig                  conf;
    private final TransactionManagerInternal    perMan;
    private final DatabaseManagerInternal       dbMan;
    private final CheckpointerInternal          cp;

    public BabuDBMock(String name, BabuDBConfig conf, LSN onDisk) throws BabuDBException {
        this.name = name;
        this.conf = conf;
        TransactionManagerMock localPersMan = new TransactionManagerMock(name, onDisk);
        this.perMan = localPersMan;
        this.dbMan = new DatabaseManagerMock();
        this.cp = new CheckpointerMock(localPersMan);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getCheckpointer()
     */
    @Override
    public CheckpointerInternal getCheckpointer() {

        Logging.logMessage(Logging.LEVEL_INFO, this,
                "Mock '%s' tried to access CP.", name);
        return cp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getDatabaseManager()
     */
    @Override
    public DatabaseManagerInternal getDatabaseManager() {

        Logging.logMessage(Logging.LEVEL_INFO, this,
                "Mock '%s' tried to access DBMan.", name);
        return dbMan;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getSnapshotManager()
     */
    @Override
    public SnapshotManagerInternal getSnapshotManager() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access SMan.", name);
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access shutdown.", name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getDBConfigFile()
     */
    @Override
    public DBConfig getDBConfigFile() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access DBConfig.", name);
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getConfig()
     */
    @Override
    public BabuDBConfig getConfig() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access BabuDBConfig.", name);
        return conf;
    }

    @Override
    public TransactionManagerInternal getTransactionManager() {

        Logging.logMessage(Logging.LEVEL_INFO, this,
                "Mock '%s' tried to access PerMan.", name);
        return perMan;
    }
    
    @Override
    public ResponseManagerInternal getResponseManager() {
        Logging.logMessage(Logging.LEVEL_INFO, this,
                "Mock '%s' tried to access RespMan.", name);
        return null;
    }

    @Override
    public void replaceTransactionManager(TransactionManagerInternal perMan) {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to replace PerMan.", name);
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getWorker(int)
     */
    @Override
    public LSMDBWorker getWorker(int dbId) {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access Worker for DB %d.", name, dbId);
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getWorkerCount()
     */
    @Override
    public int getWorkerCount() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to get worker count.", name);
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.BabuDBInternal#addPluginThread(org.xtreemfs.foundation
     * .LifeCycleThread)
     */
    @Override
    public void addPluginThread(LifeCycleThread plugin) {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to add plugin.", name);
        // TODO Auto-generated method stub
    }

    /*
     * (non-Javadoc)
     * 
     * @seeorg.xtreemfs.babudb.BabuDBInternal#init(org.xtreemfs.babudb.api.
     * StaticInitialization)
     */
    @Override
    public void init(StaticInitialization staticInit) throws BabuDBException {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to init.", name);
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#stop()
     */
    @Override
    public void stop() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to stop.", name);
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#restart()
     */
    @Override
    public LSN restart() throws BabuDBException {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to restart.", name);
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable cause) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown(boolean)
     */
    @Override
    public void shutdown(boolean graceful) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access shutdown (%s).", name, graceful);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.BabuDB#getRuntimeState(java.lang.String)
     */
    @Override
    public Object getRuntimeState(String propertyName) {
        // TODO Auto-generated method stub
        return null;
    }

}
