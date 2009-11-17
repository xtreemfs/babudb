/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.BabuDBRequestResult;
import org.xtreemfs.babudb.UserDefinedLookup;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;

public class DatabaseImpl implements Database {

    private BabuDB      dbs;
    
    private LSMDatabase lsmDB;
        
/*
 * constructors/destructors
 */
    
    /**
     * Creates a new Database.
     * 
     * @param lsmDB
     *            the underlying LSM database
     */
    public DatabaseImpl(BabuDB master, LSMDatabase lsmDB) {
        this.dbs = master;
        this.lsmDB = lsmDB;
    }
    
    /*
     * (non-Javadoc)
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
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.Database#createInsertGroup()
     */
    @Override
    public BabuDBInsertGroup createInsertGroup() throws BabuDBException {
        dbs.slaveCheck();
        
        return new BabuDBInsertGroup(lsmDB);
    }

/*
 * DB modification operations 
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.Database#singleInsert(int, byte[], byte[], java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Object> singleInsert(int indexId, byte[] key, 
            byte[] value, Object context) {

        BabuDBInsertGroup irg = new BabuDBInsertGroup(lsmDB);
        irg.addInsert(indexId, key, value);
        
        return insert(irg, context);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.Database#insert(org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup, java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Object> insert(BabuDBInsertGroup irg, Object context) {
        BabuDBRequest<Object> result = new BabuDBRequest<Object>(context);
        try {
            dbs.slaveCheck();
        } catch (BabuDBException e) {
            result.failed(e);
            return result;
        }
        
        InsertRecordGroup ins = irg.getRecord();
        int dbId = ins.getDatabaseId();
        
        LSMDBWorker w = dbs.getWorker(dbId);
        if (w != null) {
            if (Logging.isNotice()) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "insert request" +
                        " is sent to worker #" + dbId % dbs.getWorkerCount());
            }
        
            try {
                w.addRequest(new LSMDBRequest<Object>(lsmDB, result, ins));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        } else 
            directInsert(irg, result);
        
        return result;
    }
    
    /**
     * Insert an group of inserts in the context of the invoking thread.
     * 
     * @param irg
     *            the group of inserts.
     * @param listener 
     *            to notify after insert.
     */ 
    private void directInsert(final BabuDBInsertGroup irg, final 
            BabuDBRequest<Object> listener) {
        
        int numIndices = lsmDB.getIndexCount();
        
        try {
            for (InsertRecord ir : irg.getRecord().getInserts()) 
                if ((ir.getIndexId() >= numIndices) || (ir.getIndexId() < 0)) 
                    throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index " + 
                            ir.getIndexId() + " does not exist");
        } catch (BabuDBException e) {
            listener.failed(e);
            return;
        }
        
        int size = irg.getRecord().getSize();
        ReusableBuffer buf = BufferPool.allocate(size);
        irg.getRecord().serialize(buf);
        buf.flip();
        
        LogEntry e = new LogEntry(buf, new SyncListener() {
            
            public void synced(LogEntry entry) {
                entry.free();
                
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
            
            public void failed(LogEntry entry, Exception ex) {
                entry.free();
                
                listener.failed((ex != null && ex instanceof BabuDBException) 
                        ? (BabuDBException) ex : new BabuDBException(
                                ErrorCode.IO_ERROR, "could not execute insert "+
                                "because of IO problem", ex));
            }
        }, LogEntry.PAYLOAD_TYPE_INSERT);
        
        try {
            dbs.getLogger().append(e);
        } catch (InterruptedException ex) {
            listener.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                    "cannot write update to disk log", ex));
        }       
    }
    
/*
 * DB lookup operations    
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#lookup(int, byte[], java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<byte[]> lookup(int indexId, byte[] key, 
            Object context) {
        
        BabuDBRequest<byte[]> result = new BabuDBRequest<byte[]>(context);        
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice()) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "lookup request" +
                		" is sent to worker #"
                    + lsmDB.getDatabaseId() % dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<byte[]>(lsmDB, indexId, result, key, false));
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
     *                  the result listener.
     */
    private void directLookup(int indexId, byte[] key, 
            BabuDBRequest<byte[]> listener) {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
            listener.failed(new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist"));
        } else
            listener.finished(lsmDB.getIndex(indexId).lookup(key));
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#prefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Iterator<Entry<byte[], byte[]>>> prefixLookup(
            int indexId, byte[] key, Object context) {
        
        BabuDBRequest<Iterator<Entry<byte[], byte[]>>> result = 
            new BabuDBRequest<Iterator<Entry<byte[],byte[]>>>(context);
        
        try {
            dbs.slaveCheck();
        } catch (BabuDBException e) {
            result.failed(e);
            return result;
        }
        
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice() && w != null) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "lookup request" +
                		" is sent to worker #" + lsmDB.getDatabaseId() 
                		% dbs.getWorkerCount());
            }
            
            try {
                w.addRequest(new LSMDBRequest<Iterator<Entry<byte[], byte[]>>>(
                        lsmDB, indexId, result, key, true));
            } catch (InterruptedException ex) {
                result.failed(new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                        "operation was interrupted", ex));
            }
        } else
            directPrefixLookup(indexId, key, result);
        
        return result;
    }
    
    /**
     * Executes a prefix lookup, without using a worker thread.
     * 
     * @param indexId
     *            index id (0..NumIndices-1)
     * @param key
     *            the key to start the iterator at
     * @param listener 
     *            the result listener.
     */
    private void directPrefixLookup(int indexId, byte[] key, 
            BabuDBRequest<Iterator<Entry<byte[], byte[]>>> listener) {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) 
            listener.failed(new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist"));
        else
            listener.finished(lsmDB.getIndex(indexId).prefixLookup(key));
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#reversePrefixLookup(int, byte[], java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Iterator<Entry<byte[], byte[]>>> reversePrefixLookup(
            int indexId, byte[] key, Object context) {
        
        BabuDBRequest<Iterator<Entry<byte[], byte[]>>> result = 
            new BabuDBRequest<Iterator<Entry<byte[],byte[]>>>(context);
        
        try {
            dbs.slaveCheck();
        } catch (BabuDBException e) {
            result.failed(e);
            return result;
        }
        
        directReversePrefixLookup(indexId, key, result);
        
        return result;
    }
    
    /**
     * Executes a prefix lookup, without using a worker thread.
     * 
     * @param indexId
     *            index id (0..NumIndices-1)
     * @param key
     *            the key to start the iterator at
     * @param listener 
     *            the result listener.
     */
    private void directReversePrefixLookup(
            int indexId, byte[] key, BabuDBRequest<Iterator<Entry<byte[], byte[]>>> listener)  {
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) 
            listener.failed(new BabuDBException(ErrorCode.NO_SUCH_INDEX, 
                    "index does not exist"));
        else
            listener.finished(lsmDB.getIndex(indexId).prefixLookup(key, false));
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.DatabaseRO#userDefinedLookup(org.xtreemfs.babudb.UserDefinedLookup, java.lang.Object)
     */
    @Override
    public BabuDBRequestResult<Object> userDefinedLookup(UserDefinedLookup udl, 
            Object context) {
        BabuDBRequest<Object> result = new BabuDBRequest<Object>(context);
        
        try {
            dbs.slaveCheck();
        } catch (BabuDBException e) {
            result.failed(e);
            return result;
        }
        
        LSMDBWorker w = dbs.getWorker(lsmDB.getDatabaseId());
        if (w != null) {
            if (Logging.isNotice()) {
                Logging.logMessage(Logging.LEVEL_NOTICE, this, "udl request is" +
                                " sent to worker #" + lsmDB.getDatabaseId() 
                                % dbs.getWorkerCount());
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
            BabuDBRequest<Object> listener) {
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
    
    public byte[] directLookup(int indexId, int snapId, byte[] key) throws BabuDBException {
        dbs.slaveCheck();
        
        if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index does not exist");
        }
        return lsmDB.getIndex(indexId).lookup(key, snapId);
    }
    
    public Iterator<Entry<byte[], byte[]>> directPrefixLookup(int indexId, int snapId, byte[] key,
            boolean ascending) throws BabuDBException {
            dbs.slaveCheck();
            
            if ((indexId >= lsmDB.getIndexCount()) || (indexId < 0)) {
                throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index does not exist");
            }
            return lsmDB.getIndex(indexId).prefixLookup(key, snapId, ascending);
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
    public void proceedSnapshot(String destDB) throws BabuDBException, InterruptedException {
        int[] ids;
        try {
            // critical block...
            dbs.getLogger().lockLogger();
            ids = lsmDB.createSnapshot();
        } finally {
            if (dbs.getLogger().hasLock())
                dbs.getLogger().unlockLogger();
        }
        
        File dbDir = new File(dbs.getConfig().getBaseDir() + destDB);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        
        try {
            LSN lsn = lsmDB.getOndiskLSN();
            lsmDB.writeSnapshot(dbs.getConfig().getBaseDir() + destDB + File.
                    separatorChar, ids, lsn.getViewId(), lsn.getSequenceNo());
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot write snapshot: " + ex, ex);
        }
    }
    
    /**
     * Creates an in-memory snapshot of all indices in a single database. The
     * snapshot will be discarded when the system is restarted.
     * 
     * NOTE: this method should only be invoked by the framework
     * 
     * @throws BabuDBException
     *             if isSlave_check succeeded
     * @throws InterruptedException
     * @return an array with the snapshot ID for each index in the database
     */
    public int[] createSnapshot() throws BabuDBException, InterruptedException {
        dbs.slaveCheck();
        
        int[] result = null;
        try {
            // critical block...
            dbs.getLogger().lockLogger();
            result = proceedCreateSnapshot();
        } finally {
            dbs.getLogger().unlockLogger();
        }
        return result;
    }

    /**
     * Creates an in-memory snapshot of all indices in a single database. The
     * snapshot will be discarded when the system is restarted.
     * This Operation comes without slave-protection.
     * The {@link DiskLogger} has to be locked before executing this method. 
     * 
     * NOTE: this method should only be invoked by the framework
     * 
     * @return an array with the snapshot ID for each index in the database
     */
    public int[] proceedCreateSnapshot() {       
        return lsmDB.createSnapshot();
    }

    
    /**
     * Creates an in-memory snapshot of a given set of indices in a single
     * database. The snapshot will be restored when the system is restarted.
     * 
     * NOTE: this method should only be invoked by the framework
     * 
     * @throws BabuDBException
     *             if the checkpoint was not successful
     * @throws InterruptedException
     * @return an array with the snapshot ID for each index in the database
     */
    public int[] createSnapshot(SnapshotConfig snap, boolean appendLogEntry) throws BabuDBException,
        InterruptedException {
        dbs.slaveCheck();
        
        if (appendLogEntry) {
            
            // serialize the snapshot configuration
            ReusableBuffer buf = null;
            try {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                ObjectOutputStream oout = new ObjectOutputStream(bout);
                oout.writeInt(lsmDB.getDatabaseId());
                oout.writeObject(snap);
                buf = ReusableBuffer.wrap(bout.toByteArray());
                oout.close();
            } catch (IOException exc) {
                throw new BabuDBException(ErrorCode.IO_ERROR, 
                        "could not serialize snapshot configuration: " + 
                        snap.getClass(), exc);
            }
            
            DatabaseManagerImpl.metaInsert(LogEntry.PAYLOAD_TYPE_SNAP, buf, 
                    dbs.getLogger());
        }
        
        // critical block...
        if (dbs.getLogger() != null)
            dbs.getLogger().lockLogger();
        
        // create the snapshot
        int[] result = lsmDB.createSnapshot(snap.getIndices());
        
        if (dbs.getLogger() != null)
            dbs.getLogger().unlockLogger();
        
        return result;
        
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
     *             if the snapshot cannot be written,
     *             or isSlave_check was positive
     */
    public void writeSnapshot(int[] snapIds, String directory, SnapshotConfig cfg) throws BabuDBException {
        dbs.slaveCheck();
        
        proceedWriteSnapshot(snapIds, directory, cfg);
    }
    
    /**
     * Writes a snapshot to disk. Without isSlave protection.
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
    public void proceedWriteSnapshot(int[] snapIds, String directory, SnapshotConfig cfg) throws BabuDBException {
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
     *             if a snapshot cannot be written,
     *             or BabuDB is running in slave-mode.
     */
    public void writeSnapshot(int viewId, long sequenceNo, int[] snapIds) throws BabuDBException {
        dbs.slaveCheck();
        
        proceedWriteSnapshot(viewId, sequenceNo, snapIds);
    }
    
    /**
     * Writes the snapshots to disk. Without slave-protection.
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
     * unnecessary in-memory and on-disk data
     * 
     * @param viewId
     *            the viewId of the snapshot
     * @param sequenceNo
     *            the sequenceNo of the snaphot
     * @throws BabuDBException
     *             if snapshots cannot be cleaned up
     */
    public void cleanupSnapshot(final int viewId, final long sequenceNo) throws BabuDBException {
        dbs.slaveCheck();
        
        proceedCleanupSnapshot(viewId, sequenceNo);
    }
    
    /**
     * Links the indices to the latest on-disk snapshot, cleans up any
     * unnecessary in-memory and on-disk data. Without slave-protection.
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
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot clean up: " + ex, ex);
        }
    }

/*
 * getter/setter
 */
    
    /**
     * <p>
     * Replaces the currently used {@link LSMDatabase} with the given one.
     * <br>
     * Be really careful with this operation. 
     * {@link LSMDBRequest}s might get lost.
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
     * @see org.xtreemfs.babudb.lsmdb.Database#getComparators()
     */
    @Override
    public ByteRangeComparator[] getComparators() {
        return lsmDB.getComparators();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.Database#getName()
     */
    @Override
    public String getName() {
        return lsmDB.getDatabaseName();
    }
}
