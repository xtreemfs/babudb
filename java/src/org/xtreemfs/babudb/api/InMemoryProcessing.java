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
 * of the BabuDB data. This interface describes an algorithm and therefore
 * may not be stateful. 
 * 
 * @author flangner
 * @since 25.01.2011
 */
public abstract class InMemoryProcessing {
    
    /**
     * Method to serialize the request before storing it.
     * 
     * @param args - the operation's arguments.
     * 
     * @return serialized operation, ready to be stored to disk.
     * 
     * @throws BabuDBException if serialization fails.
     */
    public abstract ReusableBuffer serializeRequest(Object[] args) throws BabuDBException;
    
    /**
     * Method to deserialize the request.
     * 
     * @param serialized - the serialized operation's arguments.
     * 
     * @return deserialized operation's arguments.
     * 
     * @throws BabuDBException if deserialization fails.
     */
    public abstract Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException;
    
    /**
     * Optional method to execute before making an Operation on-disk persistent.
     * Depending on the implementation of PersistenceManager throwing an
     * exception might influence the execution of makePersistent() and after().
     * 
     * @param args - the operation's arguments.
     * 
     * @throws BabuDBException if method fails. 
     */
    public void before(Object[] args) throws BabuDBException {}
    
    /**
     * Optional method to execute after making an Operation successfully 
     * on-disk persistent. This behavior depends on the implementation of 
     * the makePersistent() method in {@link PersistenceManager}.
     * 
     * @param args - the operation's arguments.
     */
    public void after(Object[] args) {}
}
