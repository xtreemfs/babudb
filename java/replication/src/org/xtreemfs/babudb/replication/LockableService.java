/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

/**
 * Interface that describes a replication service which may be locked by the replication controller.
 * 
 * @author flangner
 * @since 02/16/2011
 */
public interface LockableService {
    
    public final static class ServiceLockedException extends Exception {

        private static final long serialVersionUID = -2079818427164355757L;       
    }
    
    /**
     * Ensures that the service is not used currently and may not be used throwing, 
     * until service is unlocked again. A steady state is assured.
     * 
     * @throws InterruptedException if waiting for pending requests to be finished has failed.
     */
    public void lock() throws InterruptedException;
        
    /**
     * Method to unlock the service. Has to be locked before.
     */
    public void unlock();
}
