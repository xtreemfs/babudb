/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api;

import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Interface between API and the core {@link BabuDB}. 
 * 
 * @author flangner
 * @since 11/03/2010
 */
public interface PersistenceManager {
    
    /**
     * @return the {@link LSN} of the latest {@link LogEntry} written by the {@link DiskLogger}.
     */
    public LSN getLatestOnDiskLSN();
        
    /**
     * Initially sets an LSN after starting the BabuDB.
     * 
     * @param initial
     */
    public void init(LSN initial);
    
    /**
     * Register some local logger instance to proceed requests at.
     * 
     * @param logger
     */
    public void setLogger (DiskLogger logger);
    
    /**
     * Method to extend the PersistenceManager with the knowledge how to handle the requests of type
     * 
     * 
     * @param type
     * @param processing
     */
    public void registerInMemoryProcessing(byte type, InMemoryProcessing processing);
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param type - of the operation.
     * @param args - the arguments for the given type of operation. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public <T> DatabaseRequestResult<T> makePersistent(byte type, Object[] args) 
            throws BabuDBException;
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param type - of the operation.
     * @param args - the arguments for the given type of operation.
     * @param serialized - the buffer of the serialized args, if previously calculated. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public <T> DatabaseRequestResult<T> makePersistent(byte type, Object[] args, 
            ReusableBuffer serialized) throws BabuDBException;
    
    /**
     * This operation tries to lock-out other services from manipulating the
     * databases persistently.
     * 
     * @throws InterruptedException if the lock could not be acquired 
     *                              successfully.
     */
    public void lockService() throws InterruptedException;
    
    /**
     * Gives the lock away. Other services are allowed to save data persistent
     * to the databases again. If this service is not owner of the lock this
     * method does not effect the lock of another service.
     */
    public void unlockService();
}
