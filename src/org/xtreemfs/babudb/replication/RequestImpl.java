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
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;

/**
 * <p><b>To get instances of that use the {@link RequestPreProcessor}!</b></p>
 * 
 * <p>Wrapper for the different requests the {@link ReplicationThread} has to
 * handle.</p>
 * 
 * @author flangner
 *
 */
class RequestImpl implements Request {
    /** the identifier for this request */
    Token                       token                   = null;

    /** the source, where the request comes from */
    InetSocketAddress           source                  = null;
    
    /** the identification of a {@link LogEntry} */
    LSN                         lsn                     = null;
  
    /** the identifier for a {@link Chunk} */
    Chunk                       chunkDetails            = null;
    
    /** the {@link LogEntry} received as answer of an request */
    LogEntry                    logEntry                = null;
    
    /** {@link Chunk} data or a serialized {@link LogEntry} to send */
    ReusableBuffer              data                    = null;

    /** for response issues and to be checked into the DB */
    LSMDBRequest                context                 = null;
    
    /** for response issues too */
    PinkyRequest                original                = null;
    
    /** for requesting the initial load by pieces */
    Map<String, List<Long>>     lsmDbMetaData           = null;
            
    /** status of an broadCast request - the expected ACKs */
    private final AtomicInteger maxReceivableACKs       = new AtomicInteger(0);
    private final AtomicInteger minExpectableACKs       = new AtomicInteger(0);
    
    RequestImpl(Token t,InetSocketAddress s){
        token = t;
        source = s;
    }
   
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#free()
     */
    public void free() {
        if (data!=null){
            BufferPool.free(data);
            data = null;
        }
        
        if (logEntry!=null){
            logEntry.free();
            logEntry = null;
        }
    }
 
/*
 * getter/setter    
 */      
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getToken()
     */
    public Token getToken() {
        return token;
    }


    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getLSN()
     */
    public LSN getLSN() {
        return lsn;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getSource()
     */
    public InetSocketAddress getSource() {
        return source;
    }
      
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getChunkDetails()
     */
    public Chunk getChunkDetails() {
        return chunkDetails;
    }
   
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getData()
     */
    public ReusableBuffer getData() {
        return data;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getContext()
     */
    public LSMDBRequest getContext() {
        return context;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getOriginal()
     */
    public PinkyRequest getOriginal() {
        return original;
    }
    

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getLsmDbMetaInformation()
     */
    public Map<String, List<Long>> getLsmDbMetaData() {
        return lsmDbMetaData;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#getLogEntry()
     */
    public LogEntry getLogEntry(){
        return logEntry;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#setMaxReceivableACKs(int)
     */
    public void setMaxReceivableACKs(int count) {
        maxReceivableACKs.set(count);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#decreaseMaxReceivableACKs(int)
     */
    public boolean decreaseMaxReceivableACKs(int count) {
        int newMax = 0;
        for (int i=0;i<count;i++)
            newMax = maxReceivableACKs.decrementAndGet();
        
        assert (newMax >= 0);
        
        return newMax >= minExpectableACKs.get();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#setMinExpectableACKs(int)
     */
    public void setMinExpectableACKs(int count) {
        minExpectableACKs.set(count);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#decreaseMinExpectableACKs()
     */
    public boolean decreaseMinExpectableACKs() {
        int remaining = minExpectableACKs.decrementAndGet();
        maxReceivableACKs.decrementAndGet();        
        return remaining == 0;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Request#failed()
     */
    public boolean failed(){
        return minExpectableACKs.get()!=0;
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String string = new String();
        if (token!=null)            string+="Token: '"+token.toString()+"',";
        if (source!=null)           string+="Source: '"+source.toString()+"',";
        if (lsn!=null)              string+="LSN: '"+lsn.toString()+"',";  
        if (logEntry!=null)         string+="LogEntry: '"+logEntry.toString()+"',"; 
        if (chunkDetails!=null)     string+="ChunkDetails: '"+chunkDetails.toString()+"',";
        if (data!=null)             string+="there is some data on the buffer,";
        if (lsmDbMetaData!=null)    string+="LSM DB metaData: "+lsmDbMetaData.toString()+"',";
        if (context!=null)          string+="context is set,";
        if (original!=null)         string+="the original PinkyRequest: "+original.toString()+"',";            
        string+="Object's id: "+super.toString();
        return string;
    }

    @Override
    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */      
    public int compareTo(Request o) {
        /*
         * Order for the pending queue in ReplicationThread.
         */
        
        if (token.compareTo(o.getToken())==0){
            if (lsn!=null) {
                if (o.getLSN()!=null)
                    return lsn.compareTo(o.getLSN());
                else
                    return +1;
            }else if (chunkDetails!=null){
                if (o.getChunkDetails()!=null)
                    return chunkDetails.compareTo(o.getChunkDetails());
                else
                    return +1;
            }
            return 0;
        }else return token.compareTo(o.getToken());
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        Request rq = (Request) o;
        
        if (token.equals(rq.getToken())){
            switch (rq.getToken()){
            case ACK:
                return (source.equals(rq.getSource())
                        && lsn.equals(rq.getLSN()));
                
            case CHUNK:
                return (source.equals(rq.getSource())
                        && chunkDetails.equals(rq.getChunkDetails()));
                
            case CHUNK_RP:
                return (source.equals(rq.getSource())
                        && chunkDetails.equals(rq.getChunkDetails()))
                        && data.equals(rq.getData());
                
            case LOAD:
                return source.equals(rq.getSource());
                
            case LOAD_RP:
                return (source.equals(rq.getSource())
                        && lsmDbMetaData.equals(rq.getLsmDbMetaData()));
                
            case REPLICA:
                return (lsn.equals(rq.getLSN()));
                
            case REPLICA_BROADCAST:
                return (lsn.equals(rq.getLSN())
                        && data.equals(rq.getData())
                        && context.equals(rq.getContext()));
                
            case RQ:
                return (source.equals(rq.getSource())
                        && lsn.equals(rq.getLSN()));
                
            default: return false;                
            }
        }return false;
    }
}
