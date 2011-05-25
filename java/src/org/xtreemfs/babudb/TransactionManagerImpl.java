/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.dev.transaction.InMemoryProcessing;
import org.xtreemfs.babudb.api.dev.transaction.OperationInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Default implementation of the {@link TransactionManagerInternal} interface using
 * the {@link DiskLogger} to let operations become persistent.
 * 
 * @author flangner
 * @since 11/03/2010
 */
class TransactionManagerImpl extends TransactionManagerInternal {
    
    private final AtomicReference<DiskLogger> diskLogger = new AtomicReference<DiskLogger>(null);
    
    /**
     * list of transaction listeners
     */
    private final List<TransactionListener>   listeners  = new LinkedList<TransactionListener>();
    
    private volatile LSN                      latestOnDisk;
    
    private final boolean                     isAsync;
    
    public TransactionManagerImpl (boolean isAsync) {
        this.isAsync = isAsync;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#init(
     *          org.xtreemfs.babudb.lsmdb.LSN)
     */
    public void init(LSN initial) {
        latestOnDisk = initial; 
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#setLogger(
     *          org.xtreemfs.babudb.log.DiskLogger)
     */
    public void setLogger(DiskLogger logger) {
        
        synchronized (diskLogger) {
            
            DiskLogger old = diskLogger.getAndSet(logger);
            if (logger == null) {
                latestOnDisk = old.getLatestLSN();
            } else {
                diskLogger.notify(); 
            }
        }
    }
      
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#makePersistent(
     *          org.xtreemfs.babudb.api.dev.transaction.TransactionInternal, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer, 
     *          org.xtreemfs.babudb.BabuDBRequestResultImpl)
     */
    @Override
    public void makePersistent(final TransactionInternal txn, ReusableBuffer payload, 
            BabuDBRequestResultImpl<Object> future) throws BabuDBException {

        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Trying to perform transaction %s ...", 
                txn.toString());
        
        try {
                    
            Object[] result = inMemory(txn, payload);
            LogEntry entry = generateLogEntry(txn, payload, future, result);
                        
            onDisk(txn, entry);
                        
            // notify listeners (async)
            if (isAsync) {
                future.finished(result, null);
                
                for (TransactionListener l : listeners) {
                    l.transactionPerformed(txn);
                }
            }
            
        } finally {
            txn.unlockWorkers();
        }
    }
    
    /**
     * Method to create a logEntry for the given transaction.
     * 
     * @param txn
     * @param payload
     * @param listener
     * 
     * @return a prepared logEntry. 
     */
   private final LogEntry generateLogEntry(final TransactionInternal txn, 
           ReusableBuffer payload, final BabuDBRequestResultImpl<Object> listener, 
           final Object[] results) {
        
        LogEntry result = new LogEntry(payload, new SyncListener() {
            
            @Override
            public void synced(LSN lsn) {
        
                try {
                    
                    if (!isAsync) {
                        BabuDBException irregs = txn.getIrregularities();
                        
                        if (irregs == null) {
                            listener.finished(results, lsn);
                        } else {
                            throw new BabuDBException(irregs.getErrorCode(), "Transaction "
                            	+ "failed at the execution of the " + (txn.size() + 1) 
                                + "th operation, because: " + irregs.getMessage(), irregs);
                        }
                    }
                } catch (BabuDBException error) {
                    
                    if (!isAsync) {
                        listener.failed(error);
                    } else {
                        Logging.logError(Logging.LEVEL_WARN, this, error);
                    }
                } finally {
                    
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "... transaction %s finished.", 
                            txn.toString());
                    
                    // notify listeners (sync)
                    if (!isAsync) {
                        for (TransactionListener l : listeners) {
                            l.transactionPerformed(txn);
                        }
                    }
                }
            }
            
            @Override
            public void failed(Exception ex) {
                if (!isAsync) {
                    listener.failed((ex != null && ex instanceof BabuDBException) ? 
                            (BabuDBException) ex : new BabuDBException(
                                    ErrorCode.INTERNAL_ERROR, ex.getMessage()));
                }
            }
        }, LogEntry.PAYLOAD_TYPE_TRANSACTION);
        
        return result;
    }
    
    /**
     * Initialize the on-disk processing. The transaction's log entry is send to the diskLogger.
     * 
     * @param txn
     * @param entry
     */
    private final void onDisk(TransactionInternal txn, LogEntry entry) throws BabuDBException {
        
        // append the entry to the DiskLogger if available, wait otherwise
        try {
            
            synchronized (diskLogger) {
                
                while (diskLogger.get() == null) {
                    diskLogger.wait();
                }
                
                diskLogger.get().append(entry);
            }
        } catch (InterruptedException ie) {
            
            if (entry != null) entry.free();
            throw new BabuDBException(ErrorCode.INTERRUPTED, "Operation " +
                        "could not have been stored persistent to disk and " +
                        "will therefore be discarded.", ie.getCause());
        } 
    }
    
    /**
     * Internal method to process the in-memory changes on BabuDB for a given transaction.
     * 
     * @param txn
     * @throws BabuDBException
     */
    private final Object[] inMemory(TransactionInternal txn, ReusableBuffer payload) 
            throws BabuDBException {
        
        List<Object> operationResults = new ArrayList<Object>();
                
        // in memory processing
        for (int i = 0; i < txn.size(); i++) {
            try {
                OperationInternal operation = txn.get(i);
                txn.lockResponsibleWorker(operation.getDatabaseName());
                operationResults.add(inMemoryProcessing.get(operation.getType()).process(operation));
                
            } catch (BabuDBException be) {
                
                // have there already been some successful executions?
                if (i > 0) {
                
                    // trim the transaction
                    txn.cutOfAt(i, be);
                    BufferPool.free(payload);
                    payload = null;
                    try {
                        payload = BufferPool.allocate(txn.getSize());
                        txn.serialize(payload);
                    } catch (IOException ioe) {
                        
                        if (payload != null) BufferPool.free(payload);
                        throw new BabuDBException(ErrorCode.IO_ERROR, ioe.getMessage(), ioe);
                    }
                } else {
                    
                    // no operation could have been executed so far
                    BufferPool.free(payload);
                    throw be;
                }
            }
        }
        
        return operationResults.toArray();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#
     *          replayTransaction(org.xtreemfs.babudb.api.dev.transaction.TransactionInternal)
     */
    @Override
    public void replayTransaction(TransactionInternal txn) throws BabuDBException {

        for (OperationInternal operation : txn) {
            
            byte type = operation.getType();
            // exclude database create/copy/delete calls from replay
            if (type != Operation.TYPE_COPY_DB && 
                type != Operation.TYPE_CREATE_DB && 
                type != Operation.TYPE_DELETE_DB) {
                
                // get processing logic
                InMemoryProcessing processing = inMemoryProcessing.get(type);
                
                // replay in-memory changes
                try {
                    processing.process(operation);   
                } catch (BabuDBException be) {
                    
                    // there might be false positives if a snapshot to delete has already been 
                    // deleted or a snapshot to create has already been created
                    if (!(type == Operation.TYPE_CREATE_SNAP && 
                            be.getErrorCode() == ErrorCode.SNAP_EXISTS) &&
                        !(type == Operation.TYPE_DELETE_SNAP && 
                            be.getErrorCode() == ErrorCode.NO_SUCH_SNAPSHOT)) {
                        
                        throw be;
                    }
                }
            }
        }
    } 

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#lockService()
     */
    @Override
    public void lockService() throws InterruptedException {
        DiskLogger logger = diskLogger.get();
        if(logger != null) {
            logger.lock();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#unlockService()
     */
    @Override
    public void unlockService() {
        
        DiskLogger logger = diskLogger.get();
        if (logger != null && logger.hasLock()) {
            logger.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#getLatestOnDiskLSN()
     */
    @Override
    public LSN getLatestOnDiskLSN() {
        DiskLogger logger = diskLogger.get();
        if (logger != null) {
            return logger.getLatestLSN();
        } else {
            return latestOnDisk;
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.TransactionManagerInternal#addTransactionListener(
     *          org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void addTransactionListener(TransactionListener listener) {
        if (listener == null) throw new NullPointerException();
        
        listeners.add(listener);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.TransactionManagerInternal#removeTransactionListener(
     *          org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void removeTransactionListener(TransactionListener listener) {
        listeners.remove(listener);
    }
}
