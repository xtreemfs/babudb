/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

/**
 * <p>Identify different types of {@link Request}s done by the {@link ReplicationThread}.</p>
 * <p>Tokens are ordered by the priority, with which they will be processed in the ReplicationThread.</p>
 * 
 * @author flangner
 *
 */

enum Token {
 
/*
 * Token for DB service routines
 */
    
    /** Create a new DB. */
    CREATE,
    
    /** Copy a DB. */
    COPY,
    
    /** Delete a DB. */
    DELETE,
  
/*
 * Regular replication and failure Tokens.
 */
    
    /** Acknowledgment from a Slave to the Master for the latest added {@link org.xtreemfs.babudb.log.LogEntry}. 
     *  Is a real request and a sub-request retrieved from the response.
     */
    ACK,
    
    /** Response for a complete database load, till a specified LSN. */
    LOAD_RP,
    
    /** Response for a file chunk, with given file name and byte range. */
    CHUNK_RP, 
    
    /** Slave-token, for receiving a replica from the master. */
    REPLICA,
    
    /** Request for a missing {@link org.xtreemfs.babudb.log.LogEntry}. */
    RQ,
    
    /** Master-token, for sending a replica to the slaves. */
    REPLICA_BROADCAST,
 
    /** Request for a complete database load, till a specified LSN. */
    LOAD,
    
    /** Request for a missing {@link Chunk} to a slave */
    CHUNK,  
}
