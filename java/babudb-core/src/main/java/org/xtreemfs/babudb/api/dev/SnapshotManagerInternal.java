/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.SnapshotManager;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;

/**
 * Interface of {@link SnapshotManager} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */

public interface SnapshotManagerInternal extends SnapshotManager {
    
    /**
     * Method to retrieve the directory of the designated snapshot.
     * 
     * @param dbName
     * @param snapshotName
     * @return the directory the designated snapshot has been stored.
     */
    public String getSnapshotDir(String dbName, String snapshotName);
    
    /**
     * Invoked by the framework when snapshot creation has completed.
     * @param dbName
     * @param snap
     * 
     * @throws BabuDBException
     */
    public void snapshotComplete(String dbName, SnapshotConfig snap) throws BabuDBException;
    
    /**
     * Method to delete all available snapshots of a database.
     * 
     * @param dbName
     * @throws BabuDBException
     */
    public void deleteAllSnapshots(String dbName) throws BabuDBException;
    
    /**
     * Terminates the {@link SnapshotManager}.
     * 
     * @throws BabuDBException if an error occurs.
     */
    public void shutdown() throws BabuDBException;
}
