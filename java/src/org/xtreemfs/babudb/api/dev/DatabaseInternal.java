/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
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
     * Replaces the currently used {@link LSMDatabase} with the given one. <br>
     * Be really careful with this operation. {@link LSMDBRequest}s might get
     * lost.
     * 
     * @param lsmDatabase
     */
    public void setLSMDB(LSMDatabase lsmDatabase);
    
/*
 * snapshot specific operations
 */
    
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
    
    /**
     * Writes the snapshots to disk.
     * 
     * @param viewId
     *            current viewId (i.e. of the last write)
     * @param sequenceNo
     *            current sequenceNo (i.e. of the last write)
     * @param snapIds
     *            the snapshot Ids (obtained via createSnapshot).
     * @throws BabuDBException
     *             if a snapshot cannot be written
     */
    public void proceedWriteSnapshot(int viewId, long sequenceNo, int[] snapIds) 
            throws BabuDBException;
    
    /**
     * Links the indices to the latest on-disk snapshot, cleans up any
     * unnecessary in-memory and on-disk data. 
     * 
     * @param viewId
     *            the viewId of the snapshot
     * @param sequenceNo
     *            the sequenceNo of the snaphot
     * @throws BabuDBException
     *             if snapshots cannot be cleaned up
     */
    public void proceedCleanupSnapshot(final int viewId, final long sequenceNo) 
            throws BabuDBException;
    
    /**
     * Creates an in-memory snapshot of all indices in a single database. The
     * snapshot will be discarded when the system is restarted. This Operation
     * comes without slave-protection. The {@link DiskLogger} has to be locked
     * before executing this method.
     * 
     * This method will not generate a {@link LogEntry}.
     * 
     * NOTE: this method should only be invoked by the framework
     * 
     * @return an array with the snapshot ID for each index in the database
     */
    public int[] proceedCreateSnapshot();
    
    /**
     * Dumps a snapshot of the database with the given directory as base dir.
     * The database snapshot is in base dir + database name
     * 
     * @param baseDir
     * @throws BabuDBException
     */
    public void dumpSnapshot(String baseDir) throws BabuDBException;
    
    /**
     * Creates an in-memory snapshot of all indices in a single database and
     * writes the snapshot to disk. Eludes the slave-check.
     * 
     * NOTE: this method should only be invoked by the replication
     * 
     * @param destDB
     *            - the name of the destination DB name.
     * 
     * @throws BabuDBException
     *             if the checkpoint was not successful
     * @throws InterruptedException
     */
    public void proceedSnapshot(String destDB) throws BabuDBException, InterruptedException;
    
    /**
     * @param indexId
     * @param snapId
     * @param key
     * @return the value that has been looked up.
     * @throws BabuDBException
     */
    public byte[] directLookup(int indexId, int snapId, byte[] key) throws BabuDBException;
    
    /**
     * @param indexId
     * @param snapId
     * @param key
     * @param ascending
     * @return range of values that have been looked up.
     * @throws BabuDBException
     */
    public ResultSet<byte[], byte[]> directPrefixLookup(int indexId, int snapId, byte[] key, 
            boolean ascending) throws BabuDBException;
    
    /** 
     * @param indexId
     * @param snapId
     * @param from
     * @param to
     * @param ascending
     * @return range of values that have been looked up.
     * @throws BabuDBException
     */
    public ResultSet<byte[], byte[]> directRangeLookup(int indexId, int snapId, byte[] from, 
            byte[] to, boolean ascending) throws BabuDBException;
}
