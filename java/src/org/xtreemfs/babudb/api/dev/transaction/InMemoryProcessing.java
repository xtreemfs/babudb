/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev.transaction;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * In-memory operations directly connected with modifying the on-disk state 
 * of the BabuDB data. This interface describes an algorithm and therefore
 * may not be stateful. 
 * 
 * @author flangner
 * @since 01/25/2011
 */
public abstract class InMemoryProcessing {
    
    /**
     * Method to deserialize the request.
     * <p>
     * ATTENTION: Operation request serialization/deserialization has been replaced by the unified 
     * transaction serialization scheme.
     * </p>
     * 
     * @param serialized - the serialized operation's arguments.
     * 
     * @return deserialized operation's arguments.
     * 
     * @throws BabuDBException if deserialization fails.
     */
    @Deprecated
    public abstract Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException;
    
    /**
     * Converts the operation retrieved from an old log entry of obsolete log file into a 
     * transaction operation.
     * 
     * @param args
     * @return a transaction for the given arguments.
     */
    @Deprecated
    public abstract OperationInternal convertToOperation(Object[] args);
    
    /**
     * Optional method to execute before making an Operation on-disk persistent.
     * Depending on the implementation of TransactionManagerInternal throwing an
     * exception might influence the execution of makePersistent() and after().
     * 
     * @param operation - operation to execute.
     * 
     * @throws BabuDBException if method fails. 
     */
    public void before(OperationInternal operation) throws BabuDBException {}
    
    /**
     * Optional method to execute while the request was already successfully queued to be made
     * persistent.
     * 
     * @param operation - operation to execute.
     */
    public void meanwhile(OperationInternal operation) {}
    
    /**
     * Optional method to execute after making an Operation successfully 
     * on-disk persistent. This behavior depends on the implementation of 
     * the makePersistent() method in {@link TransactionManagerInternal}.
     * 
     * @param operation - operation to execute.
     * 
     * @throws BabuDBException if method fails due a software failure.
     */
    public void after(OperationInternal operation) throws BabuDBException {}
}
