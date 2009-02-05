package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.foundation.pinky.PinkyRequest;

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
     * sets the number of ACKs expected to a broad cast request
     *  
     * @param count
     */
    void setACKsExpected(int count);
    
    /**
     * Decreases the sub-request-counter
     * 
     * @param n - parameter of the NSync mode.
     * @return true, if counter was decreased to 0, or n equals the number of succeeded requests, false otherwise.
     */
    boolean decreaseACKSExpected(int n);
    
    /**
     * 
     * @return true if a broadCast request failed
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