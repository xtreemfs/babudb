/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev.transaction;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.dev.transaction.OperationInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.lsmdb.BabuDBTransaction;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Internal interface for BabuDB's lightwight transactions.
 * 
 * @author flangner
 * @since 05/06/2011
 */
public abstract class TransactionInternal extends LinkedList<OperationInternal> 
        implements Transaction, Iterable<OperationInternal> {
    private static final long serialVersionUID = 1383031301195486005L;
    
    private Map<String, DatabaseRequestResult<AtomicBoolean>> databaseLockFutureMap = null;
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#createSnapshot(java.lang.String, 
     *          org.xtreemfs.babudb.snapshots.SnapshotConfig)
     */
    @Override
    public abstract TransactionInternal createSnapshot(String databaseName, SnapshotConfig config);
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#deleteSnapshot(java.lang.String, 
     *          java.lang.String)
     */
    @Override
    public abstract TransactionInternal deleteSnapshot(String databaseName, String snapshotName);
        
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#createDatabase(java.lang.String, int)
     */
    @Override
    public abstract TransactionInternal createDatabase(String databaseName, int numIndices);
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#createDatabase(java.lang.String, int, 
     *          org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public abstract TransactionInternal createDatabase(String databaseName, int numIndices, 
            ByteRangeComparator[] comparators);
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#copyDatabase(java.lang.String, 
     *          java.lang.String)
     */
    @Override
    public abstract TransactionInternal copyDatabase(String sourceName, String destinationName);
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#deleteDatabase(java.lang.String)
     */
    @Override
    public abstract TransactionInternal deleteDatabase(String databaseName);
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#insertRecord(java.lang.String, int, 
     *          byte[], byte[])
     */
    @Override
    public abstract TransactionInternal insertRecord(String databaseName, int indexId, byte[] key, 
            byte[] value);
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.transaction.Transaction#deleteRecord(java.lang.String, int, 
     *          byte[])
     */
    @Override
    public abstract TransactionInternal deleteRecord(String databaseName, int indexId, byte[] key);
    
    /* (non-Javadoc)
     * @see java.util.AbstractCollection#toString()
     */
    @Override
    public abstract String toString();
    
    /**
     * There are 3 kinds of database manipulating operations covered by transactions:
     * <ol>
     * <li>database create/copy/delete
     * <li>snapshots
     * <li>database key-value inserts/deletes
     * </ol>
     * These form different level of isolation that have to be respected by the replication for 
     * example. This method aggregates all occurrences of operation to make it possible to 
     * comprehend the level of restriction for this transaction. 
     *
     * @return an aggregation of all types of operation occurring in this transaction.
     */
    public final byte aggregateOperationTypes() {
        
        // initial value, if transaction contains no operation
        byte result = 0;
        for (Operation op : this) {
            result |= 1 << op.getType();
        }
        return result;
    }
    
    /**
     * @return a set of names of databases affected by this transaction. 
     */
    public final Set<String> databasesAffected() {
        
        Set<String> result = new HashSet<String>();
        for (Operation op : this) {
            result.add(op.getDatabaseName());
        }
        
        return result;
    }
    
    /**
     * Mapping of databases to lock futures for the worker threads being responsible for them.
     * 
     * @param databaseLockFutureMap - a map of lock futures for database workers affected by this 
     *                           transaction.
     */
    public final synchronized void updateWorkerLocks(
            Map<String, DatabaseRequestResult<AtomicBoolean>> databaseLockFutureMap) {
        
        this.databaseLockFutureMap = databaseLockFutureMap;
    }
    
    /**
     * Method to lock the worker responsible for database with databaseName.
     * 
     * @param databaseName
     * @throws BabuDBException if the lock could not have been acquired.
     */
    public final synchronized void lockResponsibleWorker(String databaseName) 
            throws BabuDBException {
        
        if (databaseLockFutureMap != null) {
            DatabaseRequestResult<AtomicBoolean> lockFuture = 
                databaseLockFutureMap.get(databaseName);
            
            if (lockFuture != null) {
                lockFuture.get();
            }
        }
    }
    
    /**
     * Method to unlock the worker threads that have been locked during this transaction.
     */
    public final synchronized void unlockWorkers() {
        if (databaseLockFutureMap != null) {
            Set<DatabaseRequestResult<AtomicBoolean>> lockFutures = 
                new HashSet<DatabaseRequestResult<AtomicBoolean>>(databaseLockFutureMap.values());
            for (DatabaseRequestResult<AtomicBoolean> lockFuture : lockFutures) {
                
                try {
                    final AtomicBoolean workerLock = lockFuture.get();
                    synchronized (workerLock) {
                        workerLock.set(false);
                        workerLock.notify();
                    }
                } catch (BabuDBException be) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "The worker lock could not have" +
                    		" been acquired for unlock.");
                }
            }
        }
    }
    
    /**
     * Counterpart to aggregateOperationTypes. Proves existence of given type.
     * 
     * @param aggregate
     * @param type
     * @return true if the transaction with the aggregate of operation types contains at least one
     *         operation of <code>type</code>, false otherwise.
     */
    public final static boolean containsOperationType(byte aggregate, byte type) {
        return ((aggregate >>> type) & 1) == 1; 
    }
    
    /**
     * Add a new group insert operation to this transaction.
     * 
     * @param databaseName - the name of the database.
     * @param irg - insert group.
     *            
     * @return the resulting Transaction.
     */
    public abstract TransactionInternal insertRecordGroup(String databaseName, 
            InsertRecordGroup irg);
    
    /**
     * Add a new group insert operation to this transaction.
     * 
     * @param databaseName - the name of the database.
     * @param db - where the group has to be inserted.
     * @param irg - insert group.
     *            
     * @return the resulting Transaction.
     */
    public abstract TransactionInternal insertRecordGroup(String databaseName, 
            InsertRecordGroup irg, LSMDatabase db);
        
    /**
     * Adds a custom operation to the transaction.
     * 
     * @param op
     */
    public abstract TransactionInternal addOperation(OperationInternal op);
    
    /**
     * @return the size of this transaction in bytes if serialized.
     * @throws IOException
     */
    public abstract int getSize() throws IOException;
    
    /**
     * Serializes the transaction to a buffer.
     * <p>
     * The following format is used:
     * <ol>
     * <li> length of the list of all database names (4 bytes)
     * <li> all database names (4 bytes for the length + #chars)
     * <li> length of the list of operations (4 bytes)
     * <li> all operations (variable size)
     * </ol>
     * 
     * Resulting buffer needs to be flip()-ed before usage.
     * </p>
     * 
     * @param buffer the buffer
     * @throws IOException
     * 
     * @return the buffer.
     */
    public abstract ReusableBuffer serialize(ReusableBuffer buffer) throws IOException;
    
    /**
     * Drops operations from position on. reason for dropping the operations.
     * 
     * @param position
     */
    public abstract void cutOfAt(int position, BabuDBException reason);
    
    /**
     * @return an exception describing irregularities that occurred during execution, or null, if 
     *         this transaction was executed correctly.
     */
    public abstract BabuDBException getIrregularities();
    
    /**
     * Deserializes the transaction.
     * 
     * @param buffer
     * @return the deserialized transaction.
     * @throws IOException
     */
    public final static TransactionInternal deserialize(ReusableBuffer buffer) throws IOException {
        
        BabuDBTransaction txn = new BabuDBTransaction();
        
        // check whether the transaction used to be empty
        if (buffer.remaining() > 0 || buffer.limit() > 0) {
            
            assert(buffer.remaining() > 0);
            
            // deserialize the list of database names
            int length = buffer.getInt();
            String[] dbNames = new String[length];
            for (int i = 0; i < length; i++) {
                byte[] bytes = new byte[buffer.getInt()];
                buffer.get(bytes);
                dbNames[i] = new String(bytes);
            }
            
            // deserialize the list of operations
            length = buffer.getInt();
            for (int i = 0; i < length; i++) {
                txn.addOperation(OperationInternal.deserialize(dbNames, buffer));
            }
        }
        return txn;
    }
}
