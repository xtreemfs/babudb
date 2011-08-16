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
import org.xtreemfs.foundation.LifeCycleThread;

/**
 * Interface of {@link Checkpointer} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */
public abstract class CheckpointerInternal extends LifeCycleThread implements Checkpointer {

    /**
     * Default constructor to preinitialize a Checkpointer object.
     */
    public CheckpointerInternal() {
        super("ChkptrThr");
    }

    /**
     * Starts the checkpointer synchronously.
     * 
     * @param logger - the disk logger
     * @param checkInterval - interval in seconds between two checks
     * @param maxLogLength - maximum log file length
     *            
     * @throws BabuDBException if initialization failed.
     */
    public abstract void init(DiskLogger logger, int checkInterval, long maxLogLength) 
            throws BabuDBException;
    
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
    public abstract LSN checkpoint(boolean incViewId) throws BabuDBException;
    
    /**
     * This method suspends the Checkpointer from taking checkpoints.
     * Not thread-safe!
     * 
     * @throws InterruptedException
     */
    public abstract void suspendCheckpointing() throws InterruptedException;
    
    /**
     * Method to manually force a checkpoint of the designated database.
     * 
     * @param dbName
     * @param snapIds
     * @param snap
     */
    public abstract void addSnapshotMaterializationRequest(String dbName, int[] snapIds, 
                                                  SnapshotConfig snap);
    
    /**
     * Method to manually remove a checkpoint materialization request for the designated database 
     * from the queue.
     * 
     * @param dbName
     * @param snapshotName
     */
    public abstract void removeSnapshotMaterializationRequest(String dbName, String snapshotName);
}