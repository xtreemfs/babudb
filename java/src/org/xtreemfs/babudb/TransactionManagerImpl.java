/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.io.IOException;
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
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import static org.xtreemfs.babudb.api.dev.transaction.TransactionInternal.*;

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
        
        DiskLogger old = diskLogger.getAndSet(logger);
        if (logger == null) {
            latestOnDisk = old.getLatestLSN();
        }
    }
      
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.TransactionManagerInternal#makePersistent(
     *          org.xtreemfs.babudb.api.dev.TransactionInternal, 
     *          org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    public <T> BabuDBRequestResultImpl<T> makePersistent(final TransactionInternal txn, 
            ReusableBuffer payload) throws BabuDBException {
              
        // before processing
        for (int i = 0; i < txn.size(); i++) {
            try {
                OperationInternal operation = txn.get(i);
                inMemoryProcessing.get(operation.getType()).before(operation);
                
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
                    
                    BufferPool.free(payload);
                    throw be;
                }
            }
        }
        
        // build the entry
        LogEntry entry = new LogEntry(payload, null, LogEntry.PAYLOAD_TYPE_TRANSACTION);
        
        // setup the result
        final BabuDBRequestResultImpl<T> result = new BabuDBRequestResultImpl<T>(entry);
        
        // define the listener
        entry.setListener(new SyncListener() {
            
            @Override
            public void synced(LogEntry entry) {
                try {
                    
                    // processing after
                    for (OperationInternal operation : txn) {
                        inMemoryProcessing.get(operation.getType()).after(operation);
                    }
                    BabuDBException irregs = txn.getIrregularities();
                    if (irregs == null) result.finished();
                    
                    // notify listeners
                    for (TransactionListener l : listeners) {
                        l.transactionPerformed(txn);
                    }
                    
                    if (irregs != null) throw new BabuDBException(irregs.getErrorCode(), 
                            "Transaction failed at the execution of the " + (txn.size() + 1) + 
                            "th operation, because: " + irregs.getMessage(), irregs);
                } catch (BabuDBException error) {
                    result.failed(error);
                } finally {
                    if (entry != null) entry.free();
                }
            }
            
            @Override
            public void failed(LogEntry entry, Exception ex) {
                result.failed((ex != null && ex instanceof BabuDBException) ? 
                        (BabuDBException) ex : new BabuDBException(
                                ErrorCode.INTERNAL_ERROR, ex.getMessage()));
                if (entry != null) entry.free();
            }
        });
        
        // append the entry to the DiskLogger
        try {
            DiskLogger logger = diskLogger.get();
            if (logger != null) {
                logger.append(entry);
            } else {
                if (entry != null) entry.free();
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "BabuDB has currently been " +
                	"stopped by an internal event. It will be back as soon as possible.");
            }
        } catch (InterruptedException ie) {
            if (entry != null) entry.free();
            throw new BabuDBException(ErrorCode.INTERRUPTED, "Operation " +
                        "could not have been stored persistent to disk and " +
                        "will therefore be discarded.", ie.getCause());
        }
        
        // processing meanwhile
        for (OperationInternal operation : txn) {
            inMemoryProcessing.get(operation.getType()).meanwhile(operation);
        }
        
        return result;
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
            if (type != TYPE_COPY_DB && 
                type != TYPE_CREATE_DB && 
                type != TYPE_DELETE_DB) {
                
                // get processing logic
                InMemoryProcessing processing = inMemoryProcessing.get(type);
                
                // replay
                try {
                    processing.before(operation);
                    processing.meanwhile(operation);
                    processing.after(operation);   
                } catch (BabuDBException be) {
                    
                    // there might be false positives if a snapshot to delete has already been 
                    // deleted or a snapshot to create has already been created
                    if (!(type == TYPE_CREATE_SNAP && 
                            be.getErrorCode() == ErrorCode.SNAP_EXISTS) &&
                        !(type == TYPE_DELETE_SNAP && 
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
            logger.lockLogger();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#unlockService()
     */
    @Override
    public void unlockService() {
        
        DiskLogger logger = diskLogger.get();
        if (logger != null && logger.hasLock()) {
            logger.unlockLogger();
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
