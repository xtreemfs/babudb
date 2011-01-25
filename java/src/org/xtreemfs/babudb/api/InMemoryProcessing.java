/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * In-memory operations directly connected with modifying the on-disk state 
 * of the BabuDB data.
 * 
 * @author flangner
 * @since 25.01.2011
 */
public abstract class InMemoryProcessing {
    
    /**
     * Method to execute before making an Operation on-disk persistent.
     * Depending on the implementation of PersistenceManager throwing an
     * exception might influence the execution of makePersistent() and 
     * after().
     * 
     * @throws BabuDBException if payload creating fails.
     * 
     * @return payload of the operation to store at the disk.
     */
    public abstract ReusableBuffer before() throws BabuDBException;
    
    /**
     * Optional method to execute after making an Operation successfully 
     * on-disk persistent. This behavior depends on the implementation of 
     * the makePersistent() method in {@link PersistenceManager}.
     */
    public void after() {}
}
