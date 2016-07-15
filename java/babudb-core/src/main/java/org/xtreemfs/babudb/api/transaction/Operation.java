/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.transaction;

import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_COPY;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_CREATE;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_DELETE;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_INSERT;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_SNAP;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_SNAP_DELETE;

/**
 * An individual operation of a transaction.
 * 
 * <p>
 * An operation represents a single modification within a transaction. Each
 * operation is attached to a type, a database name and a list of parameters.
 * </p>
 * 
 * <p>
 * There is no need for applications to instantiate operations. They are
 * implicitly created when the respective methods on a transaction are invoked.
 * However, the set of operations can be retrieved from a transaction instance
 * for analysis purposes, e.g. when a transaction listener is notified.
 * </p>
 * 
 * @author stenjan
 * @author flangner
 */
public interface Operation {
    
    /**
     * Operation type for database creations.
     */
    public static final byte TYPE_CREATE_DB    = PAYLOAD_TYPE_CREATE;
    
    /**
     * Operation type for database copy operations.
     */
    public static final byte TYPE_COPY_DB      = PAYLOAD_TYPE_COPY;
    
    /**
     * Operation type for database deletions.
     */
    public static final byte TYPE_DELETE_DB    = PAYLOAD_TYPE_DELETE;
    
    /**
     * Operation type for database snapshot creations.
     */
    public static final byte TYPE_CREATE_SNAP  = PAYLOAD_TYPE_SNAP;
    
    /**
     * Operation type for database snapshot deletions.
     */
    public static final byte TYPE_DELETE_SNAP  = PAYLOAD_TYPE_SNAP_DELETE;
    
    /**
     * Operation type for database inserts.
     */
    public static final byte TYPE_GROUP_INSERT = PAYLOAD_TYPE_INSERT;
    
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
