/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev.transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import static org.xtreemfs.babudb.api.dev.transaction.TransactionInternal.deserialize;

/**
 * Interface between API and the core {@link BabuDB}. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * Will only accept transactions.
 * 
 * @author flangner
 * @since 11/03/2010
 */
public abstract class TransactionManagerInternal {
    
    protected final Map<Byte, InMemoryProcessing> inMemoryProcessing = 
        new HashMap<Byte, InMemoryProcessing>();
    
    /**
     * @return the {@link LSN} of the latest {@link LogEntry} written by the {@link DiskLogger}.
     */
    public abstract LSN getLatestOnDiskLSN();
        
    /**
     * Initially sets an LSN after starting the BabuDB.
     * 
     * @param initial
     */
    public abstract void init(LSN initial);
    
    /**
     * Register some local logger instance to proceed requests at.
     * 
     * @param logger
     */
    public abstract void setLogger (DiskLogger logger);
    
    /**
     * Method to extend the TransactionManagerInternal with the knowledge how to handle the 
     * requests of type.
     * 
     * 
     * @param type
     * @param processing
     */
    public void registerInMemoryProcessing(byte type, InMemoryProcessing processing) {
        inMemoryProcessing.put(type, processing);
    }
    
    /**
     * @return the registered handlers for the in-memory processing of the transaction manager.
     */
    public Map<Byte, InMemoryProcessing> getProcessingLogic() {
        return inMemoryProcessing;
    }
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param transaction - the transaction-object. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public <T> BabuDBRequestResultImpl<T> makePersistent(TransactionInternal transaction) 
            throws BabuDBException {
        
        try {
            ReusableBuffer buffer = transaction.serialize(
                    BufferPool.allocate(transaction.getSize()));
            buffer.flip();
            return makePersistent(transaction, buffer);
        } catch (IOException e) {
            throw new BabuDBException (ErrorCode.IO_ERROR, e.getMessage(), e);
        }
    }
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param serialized - the buffer of the serialized transaction, if previously calculated. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public <T> BabuDBRequestResultImpl<T> makePersistent(ReusableBuffer serialized) 
            throws BabuDBException {
                
        try {
            return makePersistent(deserialize(serialized), serialized);
        } catch (IOException e) {
            throw new BabuDBException(ErrorCode.IO_ERROR, e.getMessage(), e);
        }
    }
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param transaction - the transaction-object.
     * @param serialized - the buffer of the serialized transaction, if previously calculated. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public abstract <T> BabuDBRequestResultImpl<T> makePersistent(TransactionInternal transaction, 
            ReusableBuffer serialized) throws BabuDBException;
    
    /**
     * Method to replay transactions at database restart for example.
     * 
     * @param txn
     * @throws BabuDBException
     */
    public abstract void replayTransaction(TransactionInternal txn) throws BabuDBException;
    
    /**
     * Method to replay serialized transaction log entries at database restart for example.
     * 
     * @param serializedTxn
     * @throws IOException
     * @throws BabuDBException
     */
    public void replayTransaction(LogEntry serializedTxn) throws IOException, BabuDBException {
        replayTransaction(deserialize(serializedTxn.getPayload()));
    }
    
    /**
     * This operation tries to lock-out other services from manipulating the
     * databases persistently.
     * 
     * @throws InterruptedException if the lock could not be acquired 
     *                              successfully.
     */
    public abstract void lockService() throws InterruptedException;
    
    /**
     * Gives the lock away. Other services are allowed to save data persistent
     * to the databases again. If this service is not owner of the lock this
     * method does not effect the lock of another service.
     */
    public abstract void unlockService();
    
    /**
     * Adds a new transaction listener. The listener is notified with each
     * database transaction that is successfully executed.
     * 
     * @param listener - the listener to add.
     */
    public abstract void addTransactionListener(TransactionListener listener);
    
    /**
     * Removes a transaction listener.
     * 
     * @param listener - the listener to remove.
     */
    public abstract void removeTransactionListener(TransactionListener listener);
}
