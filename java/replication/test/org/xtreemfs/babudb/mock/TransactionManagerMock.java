/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * @author flangner
 * @since 02/21/2011
 */

public class TransactionManagerMock extends TransactionManagerInternal {

    final AtomicReference<LSN> onDisk = new AtomicReference<LSN>(null);
    
    private final AtomicBoolean lock = new AtomicBoolean();
    private final String name;

    public TransactionManagerMock(String name, LSN onDisk) {
        this.onDisk.set(onDisk);
        this.name = name;
    }

    @Override
    public LSN getLatestOnDiskLSN() {
        return onDisk.get();
    }

    @Override
    public void init(LSN initial) {
        onDisk.set(initial);
    }

    @Override
    public void lockService() throws InterruptedException {

        lock.set(true);
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "TxnMan of mock '%s' has been locked.", name);
    }

    @Override
    public void makePersistent(TransactionInternal txn, ReusableBuffer serialized, 
            BabuDBRequestResultImpl<Object> future) throws BabuDBException {
		
        if (lock.get()) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Serivce has been locked!");
        } else {
            synchronized (onDisk) {
                LSN lsn = onDisk.get();
                lsn = new LSN(lsn.getViewId(), lsn.getSequenceNo() + 1L);
                onDisk.set(lsn);
                Logging.logMessage(Logging.LEVEL_ERROR, this, "TxnMan of mock '%s' has " +
                		"retrieved a new transaction to perform (%s) with LSN %s.", name, 
                		txn.toString(), lsn.toString());
            }
        }
    }

    @Override
    public void setLogger(DiskLogger logger) {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "TxnMan of mock '%s' registers a new logger (%s).", name,
                logger);
    }

    @Override
    public void unlockService() {
        lock.set(false);
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "TxnMan of mock '%s' has been unlocked.", name);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#replayTransaction(org.xtreemfs.babudb.api.dev.transaction.TransactionInternal)
     */
    @Override
    public void replayTransaction(TransactionInternal txn) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "TxnMan of mock '%s' has replayed txn %s.", name, txn.toString());
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#addTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void addTransactionListener(TransactionListener listener) {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "TxnMan of mock '%s' has added a txn listener.", name);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal#removeTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void removeTransactionListener(TransactionListener listener) {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "TxnMan of mock '%s' has removed a txn listener.", name);
    }

}
