/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api;

import org.xtreemfs.babudb.api.exceptions.BabuDBException;

/**
 * Main user application interface of BabuDB.
 * 
 * @author flangner
 * @date 11/02/2010
 */
public interface BabuDB {

    /**
     * Returns a reference to the BabuDB checkpointer. The checkpointer can be
     * used by applications to enforce the creation of a database checkpoint.
     * 
     * @return a reference to the checkpointer
     */
    public Checkpointer getCheckpointer();

    /**
     * Returns a reference to the database manager. The database manager gives
     * applications access to single databases.
     * 
     * @return a reference to the database manager
     */
    public DatabaseManager getDatabaseManager();

    /**
     * Returns a reference to the snapshot manager. The snapshot manager offers
     * applications the possibility to manage snapshots of single databases.
     * 
     * @return a reference to the snapshot manager
     */
    public SnapshotManager getSnapshotManager();
    
    /**
     * Terminates BabuDB. All threads will be terminated and all resources will
     * be freed. Note that ongoing checkpoint operations may be interrupted, but
     * the database will remain in a consistent state.
     * 
     * @throws BabuDBException
     */
    public void shutdown() throws BabuDBException;
}