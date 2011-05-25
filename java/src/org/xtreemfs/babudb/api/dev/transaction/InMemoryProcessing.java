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
     * The database changes made by the operation can be read after this method returns. Although 
     * there are no persistence guarantees on a database crash e.g. there is no entry written to the 
     * log.
     * 
     * @param operation
     * 
     * @return a possible return value for the in-memory processing of the operation, may be null.
     * 
     * @throws BabuDBException if the operation could not have been processed, due a user error.
     */
    public abstract Object process(OperationInternal operation) throws BabuDBException;
}
