/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;

/**
 * Interface of {@link Database} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */
public interface DatabaseInternal extends Database {

    /**
     * Returns the underlying LSM database implementation.
     * 
     * @return the LSM database
     */
    public LSMDatabase getLSMDB();
    
    /**
     * Writes a snapshot to disk.
     * 
     * NOTE: this method should only be invoked by the framework
     * 
     * @param snapIds
     *            the snapshot IDs obtained from createSnapshot
     * @param directory
     *            the directory in which the snapshots are written
     * @param cfg
     *            the snapshot configuration
     * @throws BabuDBException
     *             if the snapshot cannot be written
     */
    public void proceedWriteSnapshot(int[] snapIds, String directory, SnapshotConfig cfg)
        throws BabuDBException;
}
