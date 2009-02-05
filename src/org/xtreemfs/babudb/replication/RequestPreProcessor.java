/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Missing.STATUS;
import org.xtreemfs.babudb.replication.Replication.SYNC_MODUS;
import org.xtreemfs.common.buffer.BufferPool;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.logging.Logging;
import org.xtreemfs.foundation.json.JSONException;
import org.xtreemfs.foundation.json.JSONParser;
import org.xtreemfs.foundation.json.JSONString;
import org.xtreemfs.foundation.pinky.HTTPUtils;
import org.xtreemfs.foundation.pinky.PinkyRequest;
import org.xtreemfs.foundation.speedy.SpeedyRequest;

import static org.xtreemfs.babudb.replication.Token.*;

/**
 * <p>Static methods for parsing different requests into the replication abstraction.</p>
 * 
 * <p>Implementation is Thread-safe!</p>
 * 
 * @author flangner
 *
 */
class RequestPreProcessor {    
    /**
     * <p>A Single static object for creating instances of the enclosing type {@link RequestImpl}.</p>
     */
    private final static RequestPreProcessor THIS = new RequestPreProcessor();
    
    /**
     * <p>Wrapper for the different requests the {@link ReplicationThread} has to
     * handle.</p>
     * 
     * @author flangner
     *
     */
    class RequestImpl implements Request {
        /** the identifier for this request */
        private Token                   token           = null;
 
        /** the source, where the request comes from */
        private InetSocketAddress       source          = null;
        
        /** the identification of a {@link LogEntry} */
        private LSN                     lsn             = null;
      
        /** the identifier for a {@link Chunk} */
        private Chunk                   chunkDetails    = null;
        
        /** the {@link LogEntry} received as answer of an request */
        private LogEntry                logEntry        = null;
        
        /** {@link Chunk} data or a serialized {@link LogEntry} to send */
        private ReusableBuffer          data            = null;

        /** for response issues and to be checked into the DB */
        private LSMDBRequest            context         = null;
        
        /** for response issues too */
        private PinkyRequest            original        = null;
        
        /** for requesting the initial load by pieces */
        private Map<String, List<Long>> lsmDbMetaData   = null;
                
        /** status of an broadCast request - the expected ACKs */
        private final AtomicInteger     acksExpected    = new AtomicInteger(0);
        
        /** the received ACKs */
        private final AtomicInteger     succeeded       = new AtomicInteger(0);
        
        private RequestImpl(Token t,InetSocketAddress s){
            token = t;
            source = s;
        }
        
        private final AtomicBoolean     failed          = new AtomicBoolean(false);
        
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
         * @see org.xtreemfs.babudb.replication.Request#setACKsExpected(int)
         */
        public void setACKsExpected(int count) {
            acksExpected.set(count);
        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.replication.Request#decreaseACKSExpected(int)
         */
        public boolean decreaseACKSExpected(int n) {
            int remaining = acksExpected.decrementAndGet();  
            int received = succeeded.incrementAndGet();
            if (n==0) return remaining == 0;
            else return received == n; 
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.replication.Request#failed()
         */
        public boolean failed() {
            return this.failed.get();
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

    /**
     * <p>Exception that is thrown due an error while the preprocessing of a request.</p>
     * 
     * @author flangner
     *
     */
    class PreProcessException extends Exception {
        /***/
        private static final long serialVersionUID = 6210295653902709074L;
        
        int status = HTTPUtils.SC_SERVER_ERROR;
        
        /**
         * <p>The reason for this exception is here: <code>msg</code>.</p>
         * 
         * @param msg
         */
        PreProcessException(String msg) {
            super(msg);
        }
        
        /**
         * <p>The reason for this exception is here: <code>msg</code>.</p>
         * 
         * @param msg
         * @param status - from HTTPUtil. Default: SC_SERVER_ERROR
         */
        PreProcessException(String msg, int status) {
            super(msg);
            this.status = status;
        }
    }
    
    /**
     * <p>Constructs a {@link Request}, with a {@link LogEntry} <code>le</code>.</p>
     * 
     * @param le
     * @throws PreProcessException - if request could not be parsed.
     * 
     * @return the data as {@link Request} in replication abstraction.
     */
    static Request getReplicationRequest(LogEntry le) throws PreProcessException{
        if (le==null) throw THIS.new PreProcessException("The given LogEntry was >null<.");
        
        /**
         * <p>Object for generating check sums</p>
         */
        final Checksum checksum = new CRC32();
        
        RequestImpl result = THIS.new RequestImpl(REPLICA_BROADCAST,null);
        result.lsn = le.getLSN();
        result.context  = le.getAttachment();
        try {
            result.data = le.serialize(checksum);
            checksum.reset();
            le.free();
        } catch (Exception e) {
            checksum.reset();
            throw THIS.new PreProcessException("LogEntry could not be serialized because: "+e.getLocalizedMessage());
        }    
        
        assert (result.lsn!=null);
        assert (result.context!=null);
        assert (result.data!=null);
        
        return result;
    }
    
    /**
     * <p>Translates a {@link PinkyRequest} into the master-slave-replication abstraction.</p>
     * 
     * @param theRequest
     * @param frontEnd
     * @throws PreProcessException - if request could not be parsed.
     * 
     * @return the {@link Request} in replication abstraction.
     */
    @SuppressWarnings("unchecked")
    static Request getReplicationRequest(PinkyRequest theRequest,Replication frontEnd) throws PreProcessException {
        /**
         * <p>Object for generating check sums</p>
         */
        final Checksum checksum = new CRC32();       
        
        Token token = null;
        try {
            token = Token.valueOf(theRequest.requestURI);
        } catch (IllegalArgumentException e){
            throw THIS.new PreProcessException("Request had an illegal Token: "+theRequest.requestURI);
        }
        
        RequestImpl result = THIS.new RequestImpl(token,theRequest.getClientAddress());
        result.original = theRequest;
        
        String masterSecurityMsg = null;
        String slaveSecurityMsg = null;
        if (result.source!=null){
            masterSecurityMsg = "Security Exception! '"+result.source.toString()
            +"' is not a designated slave. Request will be ignored: "+result.toString();
            
            slaveSecurityMsg = "Security Exception! '"+result.source.toString()
            +"' is not the designated master. Request will be ignored: "+result.toString();
        }
        
        assert(frontEnd!=null);
        
        switch (result.token) {
        
       // for master
        case ACK:  
            if (!frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
                result.lsn = new LSN(new String(theRequest.getBody()));
            }catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("The LSN of an ACK could not be decoded because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }                
            break;
        
        case RQ:
            if (!frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
                result.lsn = new LSN(new String(theRequest.getBody()));
            }catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("The LSN of a RQ could not be decoded because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }
            break;
         
        case CHUNK:
            if (!frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
               // parse details
                JSONString jsonString = new JSONString(new String(theRequest.getBody()));
                result.chunkDetails = new Chunk((List<Object>) JSONParser.parseJSON(jsonString));
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("Chunk details could for a CHUNK_RQ not be decoded because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }
            break;    
            
        case LOAD:
            if (!frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);           
            break;   
            
       // for slave    
        case REPLICA:
            if (!frontEnd.isDesignatedMaster(result.source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
                result.logEntry = LogEntry.deserialize(ReusableBuffer.wrap(theRequest.getBody()), checksum);
                checksum.reset();
                result.lsn = result.logEntry.getLSN();                
                result.context = retrieveRequest(result, frontEnd);              
            } catch (Exception e) {
                checksum.reset();
                result.free();
                throw THIS.new PreProcessException("The data of a REPLICA could not be retrieved because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }
            break;   
            
        case CREATE:
            if (!frontEnd.isDesignatedMaster(result.source))
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);

            try {
               // parse details
                JSONString jsonString = new JSONString(new String(theRequest.getBody()));
                List<Object> data = (List<Object>) JSONParser.parseJSON(jsonString);
                
                frontEnd.dbInterface.proceedCreate((String) data.get(0),Integer.parseInt((String) data.get(1)) );
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("CREATE could not be performed because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }
            return null;
            
        case COPY:
            if (!frontEnd.isDesignatedMaster(result.source))
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
               // parse details
                JSONString jsonString = new JSONString(new String(theRequest.getBody()));
                List<Object> data = (List<Object>) JSONParser.parseJSON(jsonString);
                
                frontEnd.dbInterface.proceedCopy((String) data.get(0),(String) data.get(1), null, null);
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("COPY could not be performed because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }
            return null;
            
        case DELETE:
            if (!frontEnd.isDesignatedMaster(result.source))
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);

            try {
               // parse details
                JSONString jsonString = new JSONString(new String(theRequest.getBody()));
                List<Object> data = (List<Object>) JSONParser.parseJSON(jsonString);
                
                frontEnd.dbInterface.proceedDelete((String) data.get(0),Boolean.valueOf((String) data.get(1)));
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("DELETE could not be performed because: "+e.getLocalizedMessage()+" | Source: "+result.getSource());
            }
            return null;
            
        default:
            result.free();
            throw THIS.new PreProcessException("Unknown Request received: "+result.toString(),HTTPUtils.SC_BAD_REQUEST);
        } 
        
        Logging.logMessage(Logging.LEVEL_TRACE, frontEnd, "Request received: "+token.toString()+((result.lsn!=null) ? " "+result.lsn.toString() : ""));
        
        return result;
    }
    
    /**
     * <p>Translates a {@link SpeedyRequest} into the master-slave-replication abstraction.</p>
     * 
     * @param theResponse
     * @param frontEnd
     * @throws Exception - if request could not be preprocessed.
     * 
     * @return the {@link Request} in replication abstraction. Or null, if it was already handled.
     */
    @SuppressWarnings("unchecked")
    static Request getReplicationRequest(SpeedyRequest theResponse,Replication frontEnd) throws PreProcessException{
        Token token = null;
        try {
            token = Token.valueOf(theResponse.getURI());
        } catch (IllegalArgumentException e){
            throw THIS.new PreProcessException("Request had an illegal Token: "+theResponse.getURI());
        }
        
        /**
         * <p>Object for generating check sums</p>
         */
        final Checksum checksum = new CRC32();
        
        RequestImpl result = THIS.new RequestImpl(token,theResponse.getServer());
        
        String masterSecurityMsg = null;
        String slaveSecurityMsg = null;
        if (result.source!=null){
            masterSecurityMsg = "Security Exception! '"+result.source.toString()
            +"' is not a designated slave. Request will be ignored: "+result.toString();
            
            slaveSecurityMsg = "Security Exception! '"+result.source.toString()
            +"' is not the designated master. Request will be ignored: "+result.toString();
        }
        
        assert(frontEnd!=null);
        
        switch (result.token) {
        
       // for master:       
        case REPLICA:                                         
            try{
                Request rq = (Request) theResponse.genericAttatchment;
                if (!frontEnd.syncModus.equals(SYNC_MODUS.ASYNC)){
                    if (rq.decreaseACKSExpected(frontEnd.n)) {                   
                        rq.getContext().getListener().insertFinished(rq.getContext());
                        rq.free();
                    }
                }                   
                theResponse.freeBuffer();
                
                // ACK as sub request 
                result.token = ACK;
                result.lsn = rq.getLSN();
                result.source = theResponse.getServer();
            }catch (Exception e){
                theResponse.freeBuffer();
                throw THIS.new PreProcessException("The answer to a REPLICA by slave: "+result.source.toString()+" was not well formed: "+e.getMessage());
            }     
            if (!frontEnd.isDesignatedSlave(result.source)) {
                theResponse.freeBuffer();
                throw THIS.new PreProcessException(masterSecurityMsg);
            } 
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                // TODO count these failed for the NSYNC
                result.free();
                theResponse.freeBuffer();
                
                String msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
               
                throw THIS.new PreProcessException(msg);
            }  

            break;
            
        case CREATE:
            try{
                String msg = null;
                Request rq = (Request) theResponse.genericAttatchment;
                
                if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                    ((RequestImpl) rq).failed.set(true);
                    
                    msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                }               
                if (rq.decreaseACKSExpected(0)) {
                    synchronized(rq){
                        rq.notify();
                    }
                }
                
                theResponse.freeBuffer();
                
                if (msg!=null)
                    throw THIS.new PreProcessException(msg);
            }catch (Exception e){
                theResponse.freeBuffer();
                throw THIS.new PreProcessException("The answer to a CREATE by slave: "+result.source.toString()+" was not well formed: "+e.getMessage());
            }    
            if (!frontEnd.isDesignatedSlave(result.source)) 
                throw THIS.new PreProcessException(masterSecurityMsg);  
            return null;
            
        case COPY: 
            try{
                String msg = null;
                Request rq = (Request) theResponse.genericAttatchment;
                
                if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                    ((RequestImpl) rq).failed.set(true);
                    
                    msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                }               
                if (rq.decreaseACKSExpected(0)) {
                    synchronized(rq){
                        rq.notify();
                    }
                }
                
                theResponse.freeBuffer();
                
                if (msg!=null)
                    throw THIS.new PreProcessException(msg);
            }catch (Exception e){
                theResponse.freeBuffer();
                throw THIS.new PreProcessException("The answer to a COPY by slave: "+result.source.toString()+" was not well formed: "+e.getMessage());
            }    
            if (!frontEnd.isDesignatedSlave(result.source)) 
                throw THIS.new PreProcessException(masterSecurityMsg);  
            return null;
            
        case DELETE:
            try{
                String msg = null;
                Request rq = (Request) theResponse.genericAttatchment;
                
                if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                    ((RequestImpl) rq).failed.set(true);
                    
                    msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                }               
                if (rq.decreaseACKSExpected(0)) {
                    synchronized(rq){
                        rq.notify();
                    }
                }
                
                theResponse.freeBuffer();
                
                if (msg!=null)
                    throw THIS.new PreProcessException(msg);
            }catch (Exception e){
                theResponse.freeBuffer();
                throw THIS.new PreProcessException("The answer to a DELETE by slave: "+result.source.toString()+" was not well formed: "+e.getMessage());
            }    
            if (!frontEnd.isDesignatedSlave(result.source)) 
                throw THIS.new PreProcessException(masterSecurityMsg);  
            return null;
            
       // for slave:    
        case ACK: // ignore answers to an ACK-Request (necessary for pinky/speedy communication compatibility)
            return null;
            
        case RQ:
            if (!frontEnd.isDesignatedMaster(result.source)) {
                throw THIS.new PreProcessException(slaveSecurityMsg);
            }
           // get the lsn
            Missing<LSN> mLSN = null;
            try {                
               // make a subRequest
                mLSN = (Missing<LSN>) theResponse.genericAttatchment;
                result.source = theResponse.getServer();
                result.lsn = (LSN) mLSN.c;
                result.logEntry = LogEntry.deserialize(ReusableBuffer.wrap(theResponse.getResponseBody()), checksum);
                checksum.reset();
                result.token = REPLICA;

                result.context = retrieveRequest(result, frontEnd);
            }catch (Exception e){
                result.free();
                if (theResponse.statusCode!=HTTPUtils.SC_OKAY){
                    String msg = "RQ could not be answered by master: "+result.source.toString()+
                                 ",\r\n\t because: "; 
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                    
                    throw THIS.new PreProcessException(msg);
                }
                
                throw THIS.new PreProcessException("The answer to a RQ by master: "+result.source.toString()+" was not well formed: "+e.getMessage());
            }
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                if (mLSN!=null) mLSN.stat = STATUS.FAILED;
                result.free();
                
                String msg = "RQ ("+result.lsn.toString()+") could not be answered by master: "+result.source.toString()+
                             ",\r\n\t because: ";              
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                
                                   
            }
            theResponse.freeBuffer();
            break;      
           
        case CHUNK:
            if (!frontEnd.isDesignatedMaster(result.source)) {
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            }
            
            Missing<Chunk> chunk = null;
            try {      
               // make a subRequest
                chunk = (Missing<Chunk>) theResponse.genericAttatchment;
                result.chunkDetails = (Chunk) chunk.c;
                result.data = ReusableBuffer.wrap(theResponse.getResponseBody());
                result.token = CHUNK_RP;
            } catch (ClassCastException e) {
                result.free();
                throw THIS.new PreProcessException("Files details of a CHUNK could not be decoded for request: "+result.toString()+",\r\n\t because: "+e.getLocalizedMessage());
            }
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                if (chunk!=null) chunk.stat = STATUS.FAILED;
                result.free();
                
                String msg = "Could not get CHUNK_RP, because: ";               
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
              
                throw THIS.new PreProcessException(msg);
            } 
            theResponse.freeBuffer();
            break;
            
        case LOAD:
            if (!frontEnd.isDesignatedMaster(result.source)) {
                result.free();
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            }
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                String msg = "Could not get LOAD_RP, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;               
                throw THIS.new PreProcessException(msg);
            }
           
            try {
               // parse details & make a subRequest
                JSONString jsonString = new JSONString(new String(theResponse.getResponseBody()));
                result.lsmDbMetaData = (Map<String, List<Long>>) JSONParser.parseJSON(jsonString);              
                result.token = LOAD_RP;             
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("Files details of a LOAD_RP could not be decoded because: "+e.getLocalizedMessage());
            }
            theResponse.freeBuffer();
            break;
            
        default:
            String msg = "Unknown Response received: ";
            if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
            else msg += theResponse.statusCode;   
        
            theResponse.freeBuffer();
            throw THIS.new PreProcessException(msg);
        }
        
        Logging.logMessage(Logging.LEVEL_TRACE, frontEnd, "Response received: "+token.toString()+((result.lsn!=null) ? " "+result.lsn.toString() : ""));
        
        return result;
    }
    
    /**
     * 
     * @param rq
     * @param frontEnd
     * @return the LSMDBRequest retrieved from the logEntry
     * @throws Exception
     */
    private static LSMDBRequest retrieveRequest (Request rq,Replication frontEnd) throws Exception{
        // build a LSMDBRequest
        InsertRecordGroup irg = InsertRecordGroup.deserialize(rq.getLogEntry().getPayload());
        
        if (!frontEnd.dbInterface.databases.containsKey(irg.getDatabaseId()))
            throw THIS.new PreProcessException("Database does not exist.Load DB!");
        
        return new LSMDBRequest(frontEnd.dbInterface.databases.get(irg.getDatabaseId()),
                                          frontEnd,irg,rq);
    }

    /**
     * @param databaseName
     * @param numIndices
     * @throws PreProcessException - if informations could not be encoded.
     * @return a create {@link Request}.
     */
    static Request getReplicationRequest(String databaseName,
            int numIndices) throws PreProcessException {
        RequestImpl result = THIS.new RequestImpl(CREATE,null);
        List<Object> data = new LinkedList<Object>();
        data.add(databaseName);
        data.add(String.valueOf(numIndices));
        try {
            result.data = ReusableBuffer.wrap(JSONParser.writeJSON(data).getBytes());
        } catch (JSONException e) {
            throw THIS.new PreProcessException("CREATE request could not be encoded. Because: "+e.getMessage());
        }
        return result;
    }

    /**
     * @param sourceDB
     * @param destDB
     * @throws PreProcessException - if informations could not be encoded.
     * @return a copy {@link Request}.
     */
    static Request getReplicationRequest(String sourceDB, String destDB) throws PreProcessException {
        RequestImpl result = THIS.new RequestImpl(COPY,null);
        List<Object> data = new LinkedList<Object>();
        data.add(sourceDB);
        data.add(destDB);
        try {
            result.data = ReusableBuffer.wrap(JSONParser.writeJSON(data).getBytes());
        } catch (JSONException e) {
            throw THIS.new PreProcessException("COPY request could not be encoded. Because: "+e.getMessage());
        }
        return result;
    }

    /**
     * @param databaseName
     * @param deleteFiles
     * @throws PreProcessException - if informations could not be encoded.
     * @return a delete {@link Request}.
     */
    static Request getReplicationRequest(String databaseName,
            boolean deleteFiles) throws PreProcessException {
        RequestImpl result = THIS.new RequestImpl(DELETE,null);
        List<Object> data = new LinkedList<Object>();
        data.add(databaseName);
        data.add(String.valueOf(deleteFiles));
        try {
            result.data = ReusableBuffer.wrap(JSONParser.writeJSON(data).getBytes());
        } catch (JSONException e) {
            throw THIS.new PreProcessException("DELETE request could not be encoded. Because: "+e.getMessage());
        }        
        return result;
    }

    /**
     * dangerous architecture break-through
     * do not use this!
     * 
     * @param lsn
     * @param destination
     * @return an ACK-request for the specified LSN with the specified destination.
     */
    static Request getACKRequest(LSN lsn, InetSocketAddress destination) {
        RequestImpl result = THIS.new RequestImpl(ACK,destination);
        result.lsn = lsn;
        return result;
    }  
    
    /**
     * dangerous architecture break-through
     * do not use this!
     * 
     * @param lsn
     * @return dummy REPLICA request for testing against the pending queue.
     */
    static Request getProbeREPLICA(LSN lsn) {
        RequestImpl result = THIS.new RequestImpl(REPLICA,null);
        result.lsn = lsn;
        return result;
    }
}
