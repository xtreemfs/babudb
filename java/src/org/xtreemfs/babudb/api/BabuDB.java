/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api;

import org.xtreemfs.babudb.api.exception.BabuDBException;

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
     * Returns runtime information about the database system.
     * 
     * @param propertyName
     *            the name of the runtime state property to query
     * @return An object encapsulating certain state information. The type and
     *         data of the object depends on the queried property. If the
     *         property is undefined, <code>null</code> is returned.
     */
    public Object getRuntimeState(String propertyName);

    /**
     * Performs a graceful shutdown, which is equivalent to an invocation of
     * <code>shutdown(true)</code>.
     * 
     * @throws BabuDBException
     *             if an error occurred
     */
    public void shutdown() throws BabuDBException;

    /**
     * Terminates BabuDB. All threads will be terminated and all resources will
     * be freed.
     * 
     * <p>
     * A shutdown may be either graceful or forceful. If the database was
     * configured for asynchronous log writes, a graceful shutdown waits for all
     * pending log entries to be persistently written to disk, so as to
     * guarantee that no updates that were acknowledged will be lost.
     * </p>
     * 
     * @param graceful
     *            if <code>true</code>, the shutdown will be performed
     *            gracefully; otherwise, it will be forceful
     * @throws BabuDBException
     *             if an error occurred
     */
    public void shutdown(boolean graceful) throws BabuDBException;

}