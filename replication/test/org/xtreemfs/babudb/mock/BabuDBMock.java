/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.Checkpointer;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.SnapshotManager;
import org.xtreemfs.babudb.api.StaticInitialization;
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

    private final String                name;
    private final BabuDBConfig          conf;
    private final PersistenceManager    perMan;
    private final DatabaseManager       dbMan;

    public BabuDBMock(String name, BabuDBConfig conf) {
        this.name = name;
        this.conf = conf;
        this.perMan = new PersistenceManagerMock(name);
        this.dbMan = new DatabaseManagerMock();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getCheckpointer()
     */
    @Override
    public Checkpointer getCheckpointer() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access CP.", name);
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getDatabaseManager()
     */
    @Override
    public DatabaseManager getDatabaseManager() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access DBMan.", name);
        return dbMan;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getSnapshotManager()
     */
    @Override
    public SnapshotManager getSnapshotManager() {

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

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getPersistenceManager()
     */
    @Override
    public PersistenceManager getPersistenceManager() {

        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Mock '%s' tried to access PerMan.", name);
        return perMan;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.BabuDBInternal#replacePersistenceManager(org.xtreemfs
     * .babudb.api.PersistenceManager)
     */
    @Override
    public void replacePersistenceManager(PersistenceManager perMan) {

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

}
