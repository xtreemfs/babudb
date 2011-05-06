/*
 * Copyright (c) 2008 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api;

import org.xtreemfs.babudb.api.exception.BabuDBException;

/**
 * Interface for checkpointing support. It allows users to enforce database
 * checkpoints and to synchronously wait for a database checkpoint to complete.
 * 
 * @author stenjan, bjko
 */
public interface Checkpointer {
    
    /**
     * Creates a checkpoint of all databases. The in-memory data is merged with
     * the on-disk data and is written to a new snapshot file. Database logs are
     * truncated. This operation is thread-safe.
     * 
     * @throws BabuDBException
     *             if the checkpoint was not successful
     * @throws InterruptedException
     */
    public void checkpoint() throws BabuDBException, InterruptedException;
    
    /**
     * Wait until the current checkpoint is complete.
     * 
     * @throws InterruptedException
     */
    public void waitForCheckpoint() throws InterruptedException;
}
