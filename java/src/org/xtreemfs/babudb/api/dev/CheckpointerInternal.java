/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.Checkpointer;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;

/**
 * Interface of {@link Checkpointer} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */
public interface CheckpointerInternal extends Checkpointer {

    /**
     * @param logger
     *            the disklogger
     * @param checkInterval
     *            interval in seconds between two checks
     * @param maxLogLength
     *            maximum log file length
     */
    public void init(DiskLogger logger, int checkInterval, long maxLogLength);
    
    /**
     * Triggers the creation of a new checkpoint. This causes the checkpointer
     * to run its checkpointing mechanism, which causes all outstanding snapshot
     * materialization requests to be processed. After this has been done, the
     * checkpoint will be created.
     * 
     * The method blocks until the checkpoints (and all outstanding snapshots)
     * have been persistently written to disk.
     * 
     * @param incViewId
     *            - set true, if the viewId should be incremented.
     * 
     * @return {@link LSN} of the last {@link LogEntry} written to the {@link DiskLogger} before the 
     *         checkpoint.
     */
    public LSN checkpoint(boolean incViewId) throws BabuDBException;
    
    /**
     * This method suspends the Checkpointer from taking checkpoints.
     * 
     * @throws InterruptedException
     */
    public void suspendCheckpointing() throws InterruptedException;
    
    /**
     * Method to manually force a checkpoint of the designated database.
     * 
     * @param dbName
     * @param snapIds
     * @param snap
     */
    public void addSnapshotMaterializationRequest(String dbName, int[] snapIds, 
                                                  SnapshotConfig snap);
    
    /**
     * Method to manually remove a checkpoint materialization request for the designated database 
     * from the queue.
     * 
     * @param dbName
     * @param snapshotName
     */
    public void removeSnapshotMaterializationRequest(String dbName, String snapshotName);
    
    /**
     * Terminates the {@link Checkpointer}.
     */
    public void shutdown();
    
    /**
     * Synchronously waits for a notification indicating that the shutdown
     * procedure has been completed.
     * 
     * @throws InterruptedException
     *             if an error occurred during the shutdown procedure
     */
    public void waitForShutdown() throws InterruptedException;
}