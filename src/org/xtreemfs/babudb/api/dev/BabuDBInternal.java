/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.BabuDB;
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
     * Returns a reference to the BabuDB checkpointer. The checkpointer can be
     * used by applications to enforce the creation of a database checkpoint.
     * 
     * @return a reference to the checkpointer
     */
    public CheckpointerInternal getCheckpointer();
        
    /**
     * Returns a reference to the database manager. The database manager gives
     * applications access to single databases.
     * 
     * @return a reference to the database manager
     */
    public DatabaseManagerInternal getDatabaseManager();
    
    /**
     * Returns a reference to the snapshot manager. The snapshot manager offers
     * applications the possibility to manage snapshots of single databases.
     * 
     * @return a reference to the snapshot manager
     */
    public SnapshotManagerInternal getSnapshotManager();
    
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
     * May change during execution so always access the most common instance
     * by this method.
     * 
     * @return the {@link PersistenceManagerInternal} used by this BabuDB instance to 
     *         ensure on-disk persistence of database-modifying requests.
     */
    public PersistenceManagerInternal getPersistenceManager();
    
    /**
     * The registered {@link PersistenceManagerInternal} will be replaced by the given
     * one. This method is not thread-safe so please ensure there are no race-
     * conditions accessing the manager while execution. 
     * 
     * @param perMan
     */
    public void replacePersistenceManager(PersistenceManagerInternal perMan);
    
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
