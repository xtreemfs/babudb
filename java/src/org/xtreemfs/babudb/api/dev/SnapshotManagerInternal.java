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

/**
 * Interface of {@link SnapshotManager} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */

public interface SnapshotManagerInternal extends SnapshotManager {
    
    /**
     * Terminates the {@link SnapshotManager}.
     * 
     * @throws BabuDBException if an error occurs.
     */
    public void shutdown() throws BabuDBException;
}
