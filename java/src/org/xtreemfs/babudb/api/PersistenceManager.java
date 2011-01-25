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

/**
 * Interface between API and the core {@link BabuDB}.
 * 
 * @author flangner
 * @since 11/03/2010
 */
public abstract class PersistenceManager {
    
    /**
     * Method let some operation become persistent. Every operation executed
     * on BabuDB has to pass this method first.
     * 
     * @param <T>
     * @param type - of the operation.
     * @param processing - methods to be processed pre and post the execution
     *                     of this method. 
     * 
     * @throws BabuDBException if something went wrong.
     * 
     * @return the result listener.
     */
    public abstract <T> DatabaseRequestResult<T> makePersistent(byte type, 
            InMemoryProcessing processing) throws BabuDBException;
    
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
