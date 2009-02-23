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
import org.xtreemfs.include.foundation.pinky.PinkyRequest;

/**
 * <p>Interface for a replication request.</p>
 * 
 * <p>Replication requests are holding all necessary informations for the {@link ReplicationThread} to
 * handle it.</p>
 * 
 * @author flangner
 */

interface Request extends Comparable<Request>{

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
     * @return the identification of a {@link Chunk}.
     */
    Chunk getChunkDetails();
    
    /**
     * @return {@link Chunk} data or a {@link LogEntry} to send.
     */
    ReusableBuffer getData();
    
    /**
     * @return the context for response issues.
     */
    LSMDBRequest getContext();
        
    /**
     * @return the original request for response issues too.
     */
    PinkyRequest getOriginal();
    
    /**
     * @return the lsmDbMetaData
     */
    Map<String, List<Long>> getLsmDbMetaData();
    
    /**
     * sets the maximal number of ACKs receivable by slaves
     * 
     * @param count
     */
    void setMaxReceivableACKs(int count);
    
    /**
     * Decreases the counter for receivable sub-requests by <code>count</code>.
     * 
     * @param count
     * @return true, if the maximal number of receivable ACKs is GE than the minimal number of ACKs expected by the application. false otherwise 
     */
    boolean decreaseMaxReceivableACKs(int count);
    
    /**
     * sets the number of minimal amount of expectable ACKs for a broad cast request to be successful
     *  
     * @param count
     */
    void setMinExpectableACKs(int count);
    
    /**
     * Decreases the counter for expectable sub-requests.
     * 
     * @return true, if counter was decreased to 0, false otherwise.
     */
    boolean decreaseMinExpectableACKs();
    
    /**
     * 
     * @return true if there are still some open expected ACKs which have not been received.
     */
    boolean failed();
    
    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Request r);
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj);

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString();
}