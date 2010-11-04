/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.LifeCycleThread;

/**
 * Interface of {@link BabuDB} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @date 11/02/2010
 */
public interface BabuDBInternal extends BabuDB {

    /**
     * @return the {@link DBConfig}.
     */
    public DBConfig getDBConfigFile();

    /**
     * Returns the configuration associated with this BabuDB instance.
     * 
     * @return the configuration.
     */
    public BabuDBConfig getConfig();
    
    /**
     * 
     * @return
     */
    public PersistenceManager getPersistenceManager();
    
    /**
     * @param dbId
     * @return a worker Thread, responsible for the DB given by its ID.
     */
    public LSMDBWorker getWorker(int dbId);
    
    /**
     * Returns the number of worker threads.
     * 
     * @return the number of worker threads.
     */
    public int getWorkerCount();
    
    /**
     * Method to register a plugins thread at the BabuDB. This is necessary
     * to ensure the plugin to be shut down when BabuDB is shut down.
     * 
     * @param plugin
     */
    public void addPluginThread(LifeCycleThread plugin);
    
    /**
     * Initializes all services provided by BabuDB.
     * 
     * @param staticInit
     * @throws BabuDBException if initialization failed.
     */
    public void init(final StaticInitialization staticInit) 
        throws BabuDBException;

    /**
     * Stops all BabuDB services to be able to manipulate files without being 
     * disturbed. 
     * 
     * @see restart()
     */
    public void stop();
    
    /**
     * All services of BabuDB are restarted. Call only after stop()!
     * 
     * @see stop()
     * @throws BabuDBException
     * @return the next LSN.
     */
    public LSN restart() throws BabuDBException;
}
