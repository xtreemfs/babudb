/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api;

import org.xtreemfs.babudb.api.exception.BabuDBException;

/**
 * Interface for checkpointing support.
 * 
 * @author stender, bjko
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
    
}
