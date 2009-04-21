/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * <p>Interface for a replication request.</p>
 * 
 * <p>Replication requests are holding all necessary informations for the {@link ReplicationThread} to
 * handle it.</p>
 * 
 * @author flangner
 */

interface Request {

    /**
     * <p>Recycle {@link ReusableBuffer}s.</p>
     */
    void free();
/*
 * getter/setter    
 */
    
    /**
     * @return the identification for this request.
     */
    Token getToken();
    
    /**
     * @return the source, where the request comes from.
     */
    InetSocketAddress getSource();
    
    /**
     * @return the identification of a {@link LogEntry}.
     */
    LSN getLSN();
    
    /**
     * @return the {@link LogEntry} received as answer of an request.
     */
    LogEntry getLogEntry();
    
    /**
     * @return a {@link Chunk}.
     */
    Chunk getChunk();
    
    /**
     * @return a {@link LogEntry} to send, or meta-operation in JSON representation.
     */
    ReusableBuffer getData();
    
    /**
     * @return the context for response issues.
     */
    LSMDBRequest getContext();
    
    /**
     * @return the lsmDbMetaData
     */
    Map<String, List<Long>> getLsmDbMetaData();
    
    /**
     * @return a list of destinations, where the request should be send to.
     */
    List<InetSocketAddress> getDestinations();
    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    boolean equals(Object obj);

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    String toString();
}