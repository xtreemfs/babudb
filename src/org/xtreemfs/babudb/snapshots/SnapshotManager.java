/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.lsmdb.DatabaseRO;

public interface SnapshotManager {
    
    /**
     * Returns a read-only database backed by the snapshot with the given unique
     * name.
     * 
     * @param dbName
     *            the name of the database
     * @param snapshotName
     *            the name of the snapshot
     * @return a read-only database backed by the snapshot
     * @throws BabuDBException
     *             if the database does not exist
     */
    public DatabaseRO getSnapshotDB(String dbName, String snapshotName) throws BabuDBException;
    
    /**
     * Triggers the creation of a persistent snapshot of a database. Snapshot
     * properties can be determined in a fine-grained manner, i.e. single key
     * ranges can be selected from single indices.
     * 
     * @param dbName
     *            the name of the database to create the snapshot from
     * @param snap
     *            the snapshot configuration
     * @throws BabuDBException
     *             if snapshot creation failed
     */
    public void createPersistentSnapshot(String dbName, SnapshotConfig snap) throws BabuDBException;
    
    /**
     * Deletes a persistent snapshot.
     * 
     * @param dbName
     *            the database name
     * @param snapshotName
     *            the snapshot name
     * @throws BabuDBException
     *             if an error occurs
     */
    public void deletePersistentSnapshot(String dbName, String snapshotName) throws BabuDBException;
    
    /**
     * Returns a list of all snapshots of a given database.
     * 
     * @param dbName
     *            the database name
     * @return an array of snapshot names (may be empty if no snapshots are available)
     */
    public String[] getAllSnapshots(String dbName);
    
}
