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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.json.JSONException;
import org.xtreemfs.include.foundation.json.JSONParser;
import org.xtreemfs.include.foundation.json.JSONString;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;

import static org.xtreemfs.babudb.replication.Token.*;
import static org.xtreemfs.babudb.replication.Status.STATUS.*;

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
     * <p>A Single static object for creating instances of the enclosing type {@link PreProcessException}.</p>
     */
    private final static RequestPreProcessor THIS = new RequestPreProcessor();
    
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
        
        RequestImpl result = new RequestImpl(REPLICA_BROADCAST,null);
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
        
        RequestImpl result = new RequestImpl(token,theRequest.getClientAddress());
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
                result.context = retrieveRequest(new Status<Request>(result), frontEnd);              
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
        
        RequestImpl result = new RequestImpl(token,theResponse.getServer());
        
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
                Status<Request> rq = (Status<Request>) theResponse.genericAttatchment;
                if (rq.getValue().decreaseMinExpectableACKs()) {                  
                    rq.getValue().getContext().getListener().insertFinished(rq.getValue().getContext());
                    rq.getValue().free();
                }                 
                theResponse.freeBuffer();
                
                // ACK as sub request 
                result.token = ACK;
                result.lsn = rq.getValue().getLSN();
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
                    if (!rq.decreaseMaxReceivableACKs(1)){
                        synchronized(rq){
                            rq.notify();
                        }
                    }
                    
                    msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                } else {
                    if (rq.decreaseMinExpectableACKs()) {
                        synchronized(rq){
                            rq.notify();
                        }
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
                    if (!rq.decreaseMaxReceivableACKs(1)){
                        synchronized(rq){
                            rq.notify();
                        }
                    }
                    
                    msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                } else {              
                    if (rq.decreaseMinExpectableACKs()) {
                        synchronized(rq){
                            rq.notify();
                        }
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
                    if (!rq.decreaseMaxReceivableACKs(1)){
                        synchronized(rq){
                            rq.notify();
                        }
                    }
                    
                    msg = "Slave '"+result.source.toString()+"' did not confirm replication, because: ";
                    if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                    else msg += theResponse.statusCode;
                } else {              
                    if (rq.decreaseMinExpectableACKs()) {
                        synchronized(rq){
                            rq.notify();
                        }
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
            Status<LSN> mLSN = null;
            try {                
               // make a subRequest
                mLSN = (Status<LSN>) theResponse.genericAttatchment;
                result.source = theResponse.getServer();
                result.lsn = mLSN.getValue();
                result.logEntry = LogEntry.deserialize(ReusableBuffer.wrap(theResponse.getResponseBody()), checksum);
                checksum.reset();
                result.token = REPLICA;

                result.context = retrieveRequest(new Status<Request>(result), frontEnd);
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
                if (mLSN!=null) mLSN.setStatus(FAILED);
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
            
            Status<Chunk> chunk = null;
            try {      
               // make a subRequest
                chunk = (Status<Chunk>) theResponse.genericAttatchment;
                result.chunkDetails = chunk.getValue();
                result.data = ReusableBuffer.wrap(theResponse.getResponseBody());
                result.token = CHUNK_RP;
            } catch (ClassCastException e) {
                result.free();
                throw THIS.new PreProcessException("Files details of a CHUNK could not be decoded for request: "+result.toString()+",\r\n\t because: "+e.getLocalizedMessage());
            }
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY) {
                if (chunk!=null) chunk.setStatus(FAILED);
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
    private static LSMDBRequest retrieveRequest (Status<Request> rq,Replication frontEnd) throws Exception{
        // build a LSMDBRequest
        InsertRecordGroup irg = InsertRecordGroup.deserialize(rq.getValue().getLogEntry().getPayload());
        
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
        RequestImpl result = new RequestImpl(CREATE,null);
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
        RequestImpl result = new RequestImpl(COPY,null);
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
        RequestImpl result = new RequestImpl(DELETE,null);
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
        RequestImpl result = new RequestImpl(ACK,destination);
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
        RequestImpl result = new RequestImpl(REPLICA,null);
        result.lsn = lsn;
        return result;
    }
}
