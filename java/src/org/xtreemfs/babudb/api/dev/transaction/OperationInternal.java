/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev.transaction;

import java.io.IOException;
import java.util.LinkedList;
import java.util.SortedSet;

import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.lsmdb.BabuDBTransaction.BabuDBOperation;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Internal interface for operations of BabuDB's lightwight transactions.
 * 
 * @author flangner
 * @since 05/06/2011
 */
public abstract class OperationInternal implements Operation {
    
    public static final byte FIELD_TYPE_INTEGER    = 0;
    
    public static final byte FIELD_TYPE_SHORT      = 1;
    
    public static final byte FIELD_TYPE_LONG       = 2;
    
    public static final byte FIELD_TYPE_BOOLEAN    = 3;
    
    public static final byte FIELD_TYPE_BYTE       = 4;
    
    public static final byte FIELD_TYPE_BYTE_ARRAY = 5;
    
    public static final byte FIELD_TYPE_STRING     = 6;
    
    public static final byte FIELD_TYPE_OBJECT     = 7;
    
    public static final byte FIELD_TYPE_GROUP      = 8;
    
    /**
     * @return the size of this operation in bytes if serialized.
     * @throws IOException
     */
    public abstract int getSize() throws IOException;
        
    /**
     * Method to update the parameters while processing the operation.
     * 
     * @param params
     */
    public abstract void updateParams(Object[] params);
    
    /**
     * Method to update the database name while processing the operation.
     * 
     * @param dbName
     */
    public abstract void updateDatabaseName(String dbName);
    
    /**
     * Serializes an Operation to buffer.
     * 
     * @param dbNameSet
     * @param buffer
     * @throws IOException
     * 
     * @return the buffer.
     */
    public abstract ReusableBuffer serialize(SortedSet<String> dbNameSet, ReusableBuffer buffer) 
            throws IOException;
    
    /**
     * Deserializes an operation.
     * 
     * @param dbNameList
     * @param buffer
     * @return the deserialized operation.
     * @throws IOException
     */
    final static OperationInternal deserialize(String[] dbNameList, ReusableBuffer buffer)
            throws IOException {
    
        // deserialize type of the operation
        byte type = buffer.get();
        
        // deserialize the database name
        int dbNameIndex = buffer.getInt();
        String dbName = dbNameList[dbNameIndex];
        
        // deserialize number of parameters
        int length = buffer.getInt();
        LinkedList<Object> params = new LinkedList<Object>();
        for (int i = 0; i < length; i++) {
            params.add(BabuDBOperation.deserializeParam(buffer));
        }
        
        return new BabuDBOperation(type, dbName, (length > 0) ? params.toArray() : null);
    }
}
