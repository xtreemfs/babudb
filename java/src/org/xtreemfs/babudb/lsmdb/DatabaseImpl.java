/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.lsmdb;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.log.LogEntry.*;

public class DatabaseImpl implements DatabaseInternal {
    
    private BabuDBInternal      dbs;
    
    private LSMDatabase         lsmDB;
    
    /*
     * constructors/destructors
     */

    /**
     * Creates a new Database.
     * 
     * @param lsmDB
     *            the underlying LSM database
     */
    public DatabaseImpl(BabuDBInternal master, LSMDatabase lsmDB) {
        this.dbs = master;
        this.lsmDB = lsmDB;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        
        try {
            for (int index = 0; index < lsmDB.getIndexCount(); index++)
                lsmDB.getIndex(index).destroy();
        } catch (IOException exc) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "", exc);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#createInsertGroup()
     */
    @Override
    public DatabaseInsertGroup createInsertGroup() {
        
        return new BabuDBInsertGroup(lsmDB);
    }
    
    /*
     * DB modification operations
     */

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.lsmdb.Database#singleInsert(int, byte[], byte[],
     * java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> singleInsert(int indexId, byte[] key, 
            byte[] value, Object context) {
        
        BabuDBInsertGroup irg = new BabuDBInsertGroup(lsmDB);
        irg.addInsert(indexId, key, value);
        
        return insert(irg, context);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @seeorg.xtreemfs.babudb.lsmdb.Database#insert(org.xtreemfs.babudb.lsmdb.
     * BabuDBInsertGroup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> insert(DatabaseInsertGroup irg, 
            Object context) {
        
        final BabuDBRequestResultImpl<Object> result = 
            new BabuDBRequestResultImpl<Object>(context);
        
        InsertRecordGroup ins = ((BabuDBInsertGroup) irg).getRecord();
        int dbId = ins.getDatabaseId();
        
        LSMDBWorker w = dbs.getWorker(dbId);
        if (w != null) {
            if (Logging.isNotice()) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "insert request" 
                        + " is sent to worker #" + dbId % dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<Object>(lsmDB, result, ins));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        } else {
            directInsert((BabuDBInsertGroup) irg, result);
        }
        
        return result;
    }
    
    /**
     * Insert an group of inserts in the context of the invoking thread.
     * Proper insertion is not guaranteed, since the result of the attempt to
     * make the insert persistent is ignored, if {@link SyncMode} is ASYNC.
     * 
     * @param irg - the group of inserts.
     * @param listener - to notify after insert.
     */
    private void directInsert(BabuDBInsertGroup irg, BabuDBRequestResultImpl<Object> listener) {

        try {
            dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_INSERT, 
                           new Object[]{ irg.getRecord(), lsmDB, listener }); 
            
            // in case of asynchronous inserts, complete the request immediately
            if (dbs.getConfig().getSyncMode() == SyncMode.ASYNC) {
                
                // insert into the in-memory-tree
                for (InsertRecord ir : irg.getRecord().getInserts()) {
                    LSMTree index = lsmDB.getIndex(ir.getIndexId());
                    if (ir.getValue() != null) {
                        index.insert(ir.getKey(), ir.getValue());
                    } else {
                        index.delete(ir.getKey());
                    }
                }
                listener.finished();
            }
            
        } catch (BabuDBException e) {
            
            // if an exception occurred while writing the log, respond with an
            // error message
            listener.failed(e);
        }
    }
    
/*
 * DB lookup operations
 */

    /* (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#lookup(int, byte[],
     * java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<byte[]> lookup(int indexId, byte[] key, 
            Object context) {
        
        BabuDBRequestResultImpl<byte[]> result = 
            new BabuDBRequestResultImpl<byte[]>(context);
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice()) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "lookup request" 
                        + " is sent to worker #" 
                        + lsmDB.getDatabaseId() % dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<byte[]>(lsmDB, indexId, result, 
                        key));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        } else
            directLookup(indexId, key, result);
        
        return result;
    }
    
    /**
     * Looks up a key in the database, without using a worker thread.
     * 
     * @param indexId
     * @param key
     * @param listener
     *            the result listener.
     */
    private void directLookup(int indexId, byte[] key, 
            BabuDBRequestResultImpl<byte[]> listener) {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
            listener.failed(new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist"));
        } else
            listener.finished(lsmDB.getIndex(indexId).lookup(key));
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#prefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> prefixLookup(
            int indexId, byte[] key, Object context) {
        return prefixLookup(indexId, key, context, true);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#reversePrefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> 
            reversePrefixLookup(int indexId, byte[] key, Object context) {
        return prefixLookup(indexId, key, context, false);
    }
    
    /**
     * Performs a prefix lookup.
     * 
     * @param indexId
     * @param key
     * @param context
     * @param ascending
     * @return the request result object.
     */
    private DatabaseRequestResult<ResultSet<byte[], byte[]>> prefixLookup(
            int indexId, byte[] key, Object context, boolean ascending) {
        
        final BabuDBRequestResultImpl<ResultSet<byte[], byte[]>> result = 
            new BabuDBRequestResultImpl<ResultSet<byte[], byte[]>>(
                    context);
        
        // if there are worker threads, delegate the prefix lookup to the
        // responsible worker thread
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice() && w != null) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "lookup request" 
                        + " is sent to worker #"
                        + lsmDB.getDatabaseId() % dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<ResultSet<byte[], byte[]>>(
                        lsmDB, indexId, result, key, ascending));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        }

        // otherwise, perform a direct prefix lookup
        else {
            
            if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0))
                result.failed(new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                        "index does not exist"));
            else
                result.finished(lsmDB.getIndex(indexId).prefixLookup(key, 
                        ascending));
        }
        
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#rangeLookup(int, byte[], byte[], 
     *          java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> rangeLookup(
            int indexId, byte[] from, byte[] to, Object context) {
        return rangeLookup(indexId, from, to, context, true);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.DatabaseRO#reverseRangeLookup(int, byte[], byte[], 
     *          java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> 
            reverseRangeLookup(int indexId, byte[] from, byte[] to, 
                    Object context) {
        return rangeLookup(indexId, from, to, context, false);
    }
    
    /**
     * Performs a range lookup.
     * 
     * @param indexId
     * @param from
     * @param to
     * @param context
     * @param ascending
     * @return the request result object.
     */
    private DatabaseRequestResult<ResultSet<byte[], byte[]>> rangeLookup(
            int indexId, byte[] from, byte[] to, Object context, 
            boolean ascending) {
        
        final BabuDBRequestResultImpl<ResultSet<byte[], byte[]>> result = 
            new BabuDBRequestResultImpl<ResultSet<byte[], byte[]>>(
            context);
        
        // if there are worker threads, delegate the range lookup to the
        // responsible worker thread
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice() && w != null) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "lookup request" 
                        + " is sent to worker #"
                        + lsmDB.getDatabaseId() % dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<ResultSet<byte[], byte[]>>(
                        lsmDB, indexId, result, from, to, ascending));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        }

        // otherwise, perform a direct range lookup
        else {
            
            if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0))
                result.failed(new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                        "index does not exist"));
            else
                result.finished(lsmDB.getIndex(indexId).rangeLookup(from, to, 
                        ascending));
        }
        
        return result;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.lsmdb.DatabaseRO#userDefinedLookup(org.xtreemfs.babudb
     * .UserDefinedLookup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> userDefinedLookup(
            UserDefinedLookup udl, Object context) {
        
        final BabuDBRequestResultImpl<Object> result = 
            new BabuDBRequestResultImpl<Object>(context);
        
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice()) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "udl request is" 
                        + " sent to worker #"
                        + lsmDB.getDatabaseId() % dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<Object>(lsmDB, result, udl));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        } else
            directUserDefinedLookup(udl, result);
        
        return result;
    }
    
    /**
     * Performs a user-defined lookup, without using a worker thread.
     * 
     * @param udl
     * @param listener
     */
    private void directUserDefinedLookup(UserDefinedLookup udl, 
            BabuDBRequestResultImpl<Object> listener) {
        final LSMLookupInterface lif = new LSMLookupInterface(lsmDB);
        try {
            Object result = udl.execute(lif);
            listener.finished(result);
        } catch (BabuDBException e) {
            listener.failed(e);
        }
    }
    
/*
 * snapshot specific operations
 */

    public byte[] directLookup(int indexId, int snapId, byte[] key) 
        throws BabuDBException {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist");
        }
        return lsmDB.getIndex(indexId).lookup(key, snapId);
    }
    
    public ResultSet<byte[], byte[]> directPrefixLookup(int indexId, 
            int snapId, byte[] key, boolean ascending) throws BabuDBException {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist");
        }
        return lsmDB.getIndex(indexId).prefixLookup(key, snapId, ascending);
    }
    
    public ResultSet<byte[], byte[]> directRangeLookup(int indexId,
            int snapId, byte[] from, byte[] to, boolean ascending) 
                throws BabuDBException {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist");
        }
        return lsmDB.getIndex(indexId).rangeLookup(from, to, snapId, ascending);
    }
    
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
    public void proceedSnapshot(String destDB) throws BabuDBException, 
            InterruptedException {
        
        int[] ids;
        try {
            // critical block...
            dbs.getPersistenceManager().lockService();
            ids = lsmDB.createSnapshot();
        } finally {
            dbs.getPersistenceManager().unlockService();
        }
        
        File dbDir = new File(dbs.getConfig().getBaseDir() + destDB);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        
        try {
            LSN lsn = lsmDB.getOndiskLSN();
            lsmDB.writeSnapshot(dbs.getConfig().getBaseDir() + destDB + 
                    File.separatorChar, ids, lsn.getViewId(), 
                    lsn.getSequenceNo());
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, 
                    "cannot write snapshot: " + ex, ex.getCause());
        }
    }
    
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
    public int[] proceedCreateSnapshot() {
        return lsmDB.createSnapshot();
    }
    
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
        throws BabuDBException {
        try {
            lsmDB.writeSnapshot(directory, snapIds, cfg);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot write snapshot: " + ex, ex);
        }
    }
    
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
    public void writeSnapshot(int viewId, long sequenceNo, int[] snapIds) throws BabuDBException {
        
        proceedWriteSnapshot(viewId, sequenceNo, snapIds);
    }
    
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
    public void proceedWriteSnapshot(int viewId, long sequenceNo, int[] snapIds) throws BabuDBException {
        try {
            lsmDB.writeSnapshot(viewId, sequenceNo, snapIds);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot write snapshot: " + ex, ex);
        }
        
    }
    
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
    public void proceedCleanupSnapshot(final int viewId, final long sequenceNo) throws BabuDBException {
        try {
            lsmDB.cleanupSnapshot(viewId, sequenceNo);
        } catch (ClosedByInterruptException ex) {
            Logging.logError(Logging.LEVEL_DEBUG, this, ex);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot clean up: " + ex, ex);
        }
    }
    
    /**
     * Dumps a snapshot of the database with the given directory as base dir.
     * The database snapshot is in base dir + database name
     * 
     * @param baseDir
     * @throws BabuDBException
     */
    public void dumpSnapshot(String baseDir) throws BabuDBException {
        // destination directory of this
        baseDir = baseDir.endsWith(File.separator) ? baseDir : baseDir + File.separator;
        String destDir = baseDir + lsmDB.getDatabaseName();
        
        try {
            int ids[] = lsmDB.createSnapshot();
            LSN lsn = lsmDB.getOndiskLSN();
            lsmDB.writeSnapshot(destDir, ids, lsn.getViewId(), lsn.getSequenceNo());
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot write snapshot: " + ex, ex);
        }
    }
    
    /*
     * getter/setter
     */

    /**
     * <p>
     * Replaces the currently used {@link LSMDatabase} with the given one. <br>
     * Be really careful with this operation. {@link LSMDBRequest}s might get
     * lost.
     * </p>
     * 
     * @param lsmDatabase
     */
    public void setLSMDB(LSMDatabase lsmDatabase) {
        this.lsmDB = lsmDatabase;
    }
    
    /**
     * Returns the underlying LSM database implementation.
     * 
     * @return the LSM database
     */
    public LSMDatabase getLSMDB() {
        return lsmDB;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.lsmdb.Database#getComparators()
     */
    @Override
    public ByteRangeComparator[] getComparators() {
        return lsmDB.getComparators();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.lsmdb.Database#getName()
     */
    @Override
    public String getName() {
        return lsmDB.getDatabaseName();
    }
    
}
