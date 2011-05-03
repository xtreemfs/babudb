/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Interface between API and the core {@link BabuDB}. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 11/03/2010
 */
public abstract class PersistenceManagerInternal {
    
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
     * Method to extend the PersistenceManagerInternal with the knowledge how to handle the requests of type
     * 
     * 
     * @param type
     * @param processing
     */
    public void registerInMemoryProcessing(byte type, InMemoryProcessing processing) {
        this.inMemoryProcessing.put(type, processing);
    }
    
    /**
     * @return the registered handlers for the in-memory processing of the persistence manager.
     */
    public Map<Byte, InMemoryProcessing> getProcessingLogic() {
        return inMemoryProcessing;
    }
    
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
    public <T> BabuDBRequestResultImpl<T> makePersistent(byte type, Object[] args) 
            throws BabuDBException {
        
        return makePersistent(type, args, inMemoryProcessing.get(type).serializeRequest(args));
    }
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param type - of the operation.
     * @param serialized - the buffer of the serialized args, if previously calculated. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public <T> BabuDBRequestResultImpl<T> makePersistent(byte type, ReusableBuffer serialized) 
            throws BabuDBException {
        
        return makePersistent(type, inMemoryProcessing.get(type).deserializeRequest(serialized), 
                serialized);
    }
    
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
    public abstract <T> BabuDBRequestResultImpl<T> makePersistent(byte type, Object[] args, 
            ReusableBuffer serialized) throws BabuDBException;
    
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
}
