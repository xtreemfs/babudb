/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.transaction;

/**
 * An individual operation of a transaction.
 * 
 * @author stenjan
 * @author flangner
 */
public interface Operation {
    
    /**
     * Returns the operation type.
     * 
     * @return the operation type
     */
    public byte getType();
    
    /**
     * Returns the name of the database which the operation is assigned to.
     * 
     * @return the database name
     */
    public String getDatabaseName();
    
    /**
     * Returns the array of parameters assigned to the operation.
     * 
     * @return the parameters
     */
    public Object[] getParams();
    
}
