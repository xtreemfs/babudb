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

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationThread.ReplicationException;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.json.JSONException;
import org.xtreemfs.include.foundation.json.JSONParser;
import org.xtreemfs.include.foundation.json.JSONString;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;

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
        InetSocketAddress source = null;
        if (theRequest.getClientAddress()!=null)
            source = frontEnd.retrieveSocketAddress(theRequest.getClientAddress().getAddress());
        
        RequestImpl result = new RequestImpl(token,source);
        
        String masterSecurityMsg = null;
        String slaveSecurityMsg = null;
        if (result.source!=null){
            masterSecurityMsg = "Security Exception! '"+result.source.toString()
            +"' is not a designated slave. Request will be ignored: "+result.toString();
            
            slaveSecurityMsg = "Security Exception! '"+result.source.toString()
            +"' is not the designated master. Request will be ignored: "+result.toString();
        }
        
        assert(frontEnd!=null) : "Reference to the replication interface missing.";
        
        switch (result.token) {
        
       // for master
        case ACK:  
            if (!frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
                result.lsn = new LSN(new String(theRequest.getBody()));
    
                if (frontEnd.slavesStatus.update(source,result.lsn))
                    if (frontEnd.dbInterface.dbCheckptr!=null) // otherwise the checkIntervall is 0
                        frontEnd.dbInterface.dbCheckptr.designateRecommendedCheckpoint(frontEnd.slavesStatus.getLatestCommonLSN());
            }catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("The LSN of an ACK could not be decoded because: "+e.getMessage()+" | Source: "+result.getSource());
            }                
            return null;
        
        case RQ:
            if (!frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
                result.lsn = new LSN(new String(theRequest.getBody()));
            }catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("The LSN of a RQ could not be decoded because: "+e.getMessage()+" | Source: "+result.getSource());
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
                throw THIS.new PreProcessException("Chunk details could for a CHUNK_RQ not be decoded because: "+e.getMessage()+" | Source: "+result.getSource());
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
                
                assert (result.lsn!=null) : "The LSN of a received replica cannot be null!";
            } catch (Exception e) {
                checksum.reset();
                result.free();
                throw THIS.new PreProcessException("The data of a REPLICA could not be retrieved because: "+e.getMessage()+" | Source: "+result.getSource().toString());
            }
            break; 
            
        case LOAD_RP:
            if (!frontEnd.isDesignatedMaster(result.source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
                // parse details
                JSONString jsonString = new JSONString(new String(theRequest.getBody()));
                result.lsmDbMetaData = (Map<String, List<Long>>) JSONParser.parseJSON(jsonString);
            	
                assert(result.lsmDbMetaData!=null);
            } catch (Exception e){
            	result.free();
            	throw THIS.new PreProcessException("The data of a LOAD_RP could not be retrieved because: "+e.getMessage()+" | Source: "+result.getSource().toString());
            }
            break;
            
        case CHUNK_RP:
            if (!frontEnd.isDesignatedMaster(result.source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            JSONString jsonString = new JSONString(new String(theRequest.getBody()));
            List<Object> data;
            try {
		data = (List<Object>) JSONParser.parseJSON(jsonString);
	        result.data = (byte[]) data.remove(data.size()-1);
	        result.chunkDetails = new Chunk(data);
            } catch (JSONException e) {
                result.free();
                throw THIS.new PreProcessException("CHUNK_RP could not be performed because: "+e.getMessage()+" | Source: "+result.getSource());
            }

            break;
            
        case CREATE:
            if (!frontEnd.isDesignatedMaster(result.source))
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);

            try {
               // parse details
                jsonString = new JSONString(new String(theRequest.getBody()));
                data = (List<Object>) JSONParser.parseJSON(jsonString);
                
                frontEnd.dbInterface.proceedCreate((String) data.get(0),Integer.parseInt((String) data.get(1)) );
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("CREATE could not be performed because: "+e.getMessage()+" | Source: "+result.getSource());
            }
            return null;
            
        case COPY:
            if (!frontEnd.isDesignatedMaster(result.source))
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            try {
               // parse details
                jsonString = new JSONString(new String(theRequest.getBody()));
                data = (List<Object>) JSONParser.parseJSON(jsonString);
                
                frontEnd.dbInterface.proceedCopy((String) data.get(0),(String) data.get(1), null, null);
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("COPY could not be performed because: "+e.getMessage()+" | Source: "+result.getSource());
            }
            return null;
            
        case DELETE:
            if (!frontEnd.isDesignatedMaster(result.source))
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);

            try {
               // parse details
                jsonString = new JSONString(new String(theRequest.getBody()));
                data = (List<Object>) JSONParser.parseJSON(jsonString);
                
                frontEnd.dbInterface.proceedDelete((String) data.get(0),Boolean.valueOf((String) data.get(1)));
            } catch (Exception e) {
                result.free();
                throw THIS.new PreProcessException("DELETE could not be performed because: "+e.getMessage()+" | Source: "+result.getSource());
            }
            return null;
            
        case REPLICA_NA:  
            if (!frontEnd.isDesignatedMaster(result.source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
             // make a load request
            result.token = LOAD_RQ;
            Logging.logMessage(Logging.LEVEL_WARN, THIS, "Requested logEntry was not found; DB will be loaded soon.");
        	break;
        	
        case CHUNK_NA:
            if (!frontEnd.isDesignatedMaster(result.source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            // make a load request
            result.token = LOAD_RQ;
            Logging.logMessage(Logging.LEVEL_WARN, THIS, "Requested chunk was not found; DB will be loaded soon.");
        	break;
            
       // shared     
        case STATE: 
            if (!frontEnd.isDesignatedMaster(result.source) && !frontEnd.isDesignatedSlave(result.source))
                throw THIS.new PreProcessException(masterSecurityMsg+"\nAND\n"+slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            break;
            
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
     * @throws PreProcessException if request could not be preprocessed.
     */
    @SuppressWarnings("unchecked")
    static void getReplicationRequest(SpeedyRequest theResponse,Replication frontEnd) throws PreProcessException{
        Token token = null;
        try {
            token = Token.valueOf(theResponse.getURI());
        } catch (IllegalArgumentException e){
            throw THIS.new PreProcessException("Request had an illegal Token: "+theResponse.getURI());
        }
        
        InetSocketAddress source = null;
        if (theResponse.getServer()!=null) source = frontEnd.retrieveSocketAddress(theResponse.getServer().getAddress());
        
        String masterSecurityMsg = null;
        String slaveSecurityMsg = null;
        if (source!=null){
            masterSecurityMsg = "Security Exception! '"+source.toString()
            +"' is not a designated slave."+token.toString()+"-request will be ignored!";
            
            slaveSecurityMsg = "Security Exception! '"+source.toString()
            +"' is not the designated master."+token.toString()+"-request will be ignored!";
        }
        
        assert(frontEnd!=null) : "Reference to the replication interface missing.";
        assert(theResponse!=null) : "Null-like response is not allowed.";
        
        Status<RequestImpl> rq = null;
        
        switch (token) {
        
       // for master:       
        case REPLICA:   
            if (!frontEnd.isDesignatedSlave(source))
                throw THIS.new PreProcessException(masterSecurityMsg);

            rq = (Status<RequestImpl>) theResponse.genericAttatchment;
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY && rq!=null) {                  
                String msg = "Slave '"+source.toString()+"' did not confirm replication, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
                try {
                    rq.failed(msg, frontEnd.maxTries); 
                } catch (ReplicationException re) {
                    throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                }
            }
            break;
            
        case CREATE:
            if (!frontEnd.isDesignatedSlave(source))
                throw THIS.new PreProcessException(masterSecurityMsg); 
            
            rq = (Status<RequestImpl>) theResponse.genericAttatchment;
            
            // if the original request (CREATE) is missing, it seems to be obsolete. there is nothing to do.
            if (rq==null) break;
            
            if (theResponse.statusCode==HTTPUtils.SC_OKAY) {    
                try {
                    rq.finished();
                } catch (Exception e) {
                    try {
                        rq.failed("CREATE could not be finished because: "+e.getMessage(), frontEnd.maxTries);
                    } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                    }
                }
            } else {
                String msg = "Slave '"+source.toString()+"' did not confirm replication, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
                try {
                    rq.failed(msg, frontEnd.maxTries); 
                } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                }
            }
            break;
            
        case COPY: 
            if (!frontEnd.isDesignatedSlave(source))
                throw THIS.new PreProcessException(masterSecurityMsg); 
            
            rq = (Status<RequestImpl>) theResponse.genericAttatchment;
            
            // if the original request (COPY) is missing, it seems to be obsolete. there is nothing to do.
            if (rq==null) break;
            
            if (theResponse.statusCode==HTTPUtils.SC_OKAY) {    
                try {
                    rq.finished();
                } catch (Exception e) {
                    try {
                        rq.failed("COPY could not be finished because: "+e.getMessage(), frontEnd.maxTries);
                    } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                    }
                }
            } else {
                String msg = "Slave '"+source.toString()+"' did not confirm replication, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
                try {
                    rq.failed(msg, frontEnd.maxTries); 
                } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                }
            }
            break;
            
        case DELETE:
            if (!frontEnd.isDesignatedSlave(source))
                throw THIS.new PreProcessException(masterSecurityMsg); 
            
            rq = (Status<RequestImpl>) theResponse.genericAttatchment;
            
            // if the original request (DELETE) is missing, it seems to be obsolete. there is nothing to do.
            if (rq==null) break;
            
            if (theResponse.statusCode==HTTPUtils.SC_OKAY) {    
                try {
                    rq.finished();
                } catch (Exception e) {
                    try {
                        rq.failed("DELETE could not be finished because: "+e.getMessage(), frontEnd.maxTries);
                    } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                    }
                }
            } else {
                String msg = "Slave '"+source.toString()+"' did not confirm replication, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
                try {
                    rq.failed(msg, frontEnd.maxTries); 
                } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                }
            }
            break;
            
       // for slave:    
        case ACK: // ignore answers to an ACK-Request (necessary for pinky/speedy communication compatibility)
            break;
            
        case RQ:
            if (!frontEnd.isDesignatedMaster(source))
                throw THIS.new PreProcessException(slaveSecurityMsg);
            
            Status<LSN> mLSN = (Status<LSN>) theResponse.genericAttatchment;          
           
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY && mLSN!=null) {
                String msg = "The server does not respond ,\r\n\t because: "; 
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
            	try {
                    mLSN.failed(msg, frontEnd.maxTries); 
            	} catch (ReplicationException re) {
            		throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
            	}  
            }
            break;      
           
        case CHUNK:
            if (!frontEnd.isDesignatedMaster(source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            Status<Chunk> chunk = (Status<Chunk>) theResponse.genericAttatchment;
            
            if (theResponse.statusCode!=HTTPUtils.SC_NOT_FOUND && chunk!=null){
                String msg = "The server does not respond ,\r\n\t because: "; 
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
            	try {
                    chunk.failed(msg, frontEnd.maxTries);
            	} catch (ReplicationException re) {
            		throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
            	}
            }
            break;
            
        case LOAD:
            if (!frontEnd.isDesignatedMaster(source)) 
                throw THIS.new PreProcessException(slaveSecurityMsg,HTTPUtils.SC_UNAUTHORIZED);
            
            rq = (Status<RequestImpl>) theResponse.genericAttatchment;
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY && rq!=null) {
                String msg = "Could not get LOAD_RP, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode; 
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
            	try {
                    rq.failed(msg, frontEnd.maxTries); 
            	} catch (ReplicationException re) {
            		throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
            	}
            }     
            break;
       
       // shared:
        case STATE:
            if (!frontEnd.isDesignatedSlave(source) && !frontEnd.isDesignatedMaster(source))
                throw THIS.new PreProcessException(masterSecurityMsg+"/nAND/n"+slaveSecurityMsg); 

            rq = (Status<RequestImpl>) theResponse.genericAttatchment;
            
            if (theResponse.statusCode!=HTTPUtils.SC_OKAY && rq!=null) {
                String msg = "Slave '"+source.toString()+"' did not send it's state, because: ";
                if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
                else msg += theResponse.statusCode;
                Logging.logMessage(Logging.LEVEL_WARN, THIS, msg);
                
                try {
                    rq.failed(msg, frontEnd.maxTries); 
                } catch (ReplicationException re) {
                        throw THIS.new PreProcessException("Request could not be marked as failed, because: "+re.getMessage());
                }   
            }
            break;
            
        default:
            String msg = "Unknown Response received: ";
            if (theResponse.getResponseBody()!=null) msg += new String(theResponse.getResponseBody());
            else msg += theResponse.statusCode;   
        
            throw THIS.new PreProcessException(msg);
        }
        
        Logging.logMessage(Logging.LEVEL_TRACE, frontEnd, "Response received: "+token.toString()+" from: "+source.toString());
    }

    /**
     * @param databaseName
     * @param numIndices
     * @param slaves
     * @throws PreProcessException - if informations could not be encoded.
     * @return a create {@link Request}.
     */
    static Request getReplicationRequest(String databaseName,
            int numIndices, List<InetSocketAddress> slaves) throws PreProcessException {
        RequestImpl result = new RequestImpl(CREATE);
        List<Object> data = new LinkedList<Object>();
        data.add(databaseName);
        data.add(String.valueOf(numIndices));
        try {
            result.data = JSONParser.writeJSON(data).getBytes();
            result.destinations = slaves;
        } catch (JSONException e) {
            throw THIS.new PreProcessException("CREATE request could not be encoded. Because: "+e.getMessage());
        }
        return result;
    }

    /**
     * @param sourceDB
     * @param destDB
     * @param slaves
     * @throws PreProcessException - if informations could not be encoded.
     * @return a copy {@link Request}.
     */
    static Request getReplicationRequest(String sourceDB, String destDB,
    		List<InetSocketAddress> slaves) throws PreProcessException {
        RequestImpl result = new RequestImpl(COPY);
        List<Object> data = new LinkedList<Object>();
        data.add(sourceDB);
        data.add(destDB);
        try {
            result.data = JSONParser.writeJSON(data).getBytes();
            result.destinations = slaves;
        } catch (JSONException e) {
            throw THIS.new PreProcessException("COPY request could not be encoded. Because: "+e.getMessage());
        }
        return result;
    }

    /**
     * @param databaseName
     * @param deleteFiles
     * @param slaves
     * @throws PreProcessException - if informations could not be encoded.
     * @return a delete {@link Request}.
     */
    static Request getReplicationRequest(String databaseName,
            boolean deleteFiles, List<InetSocketAddress> slaves) throws PreProcessException {
        RequestImpl result = new RequestImpl(DELETE);
        List<Object> data = new LinkedList<Object>();
        data.add(databaseName);
        data.add(String.valueOf(deleteFiles));
        try {
            result.data = JSONParser.writeJSON(data).getBytes();
            result.destinations = slaves;
        } catch (JSONException e) {
            throw THIS.new PreProcessException("DELETE request could not be encoded. Because: "+e.getMessage());
        }        
        return result;
    }
    
    /**
     * @param destinations
     * @return a state {@link Request}.
     */
    static Request getReplicationRequest(List<InetSocketAddress> destinations) {
    	RequestImpl result = new RequestImpl(STATE_BROADCAST,null);
    	result.destinations = destinations;
    	result.lsn = new LSN(0,0L);
    	return result;
    }
    
    /**
     * <p>Constructs a {@link Request}, with a {@link LogEntry} <code>le</code>.</p>
     * 
     * @param le
     * @param slaves
     * @throws PreProcessException - if request could not be parsed.
     * 
     * @return the data as {@link Request} in replication abstraction.
     */
    static Request getReplicationRequest(LogEntry le, List<InetSocketAddress> slaves) throws PreProcessException{
        if (le==null) throw THIS.new PreProcessException("The given LogEntry was >null<.");
        
        /**
         * <p>Object for generating check sums</p>
         */
        final Checksum checksum = new CRC32();
        
        RequestImpl result = new RequestImpl(REPLICA_BROADCAST,null);
        result.lsn = le.getLSN();
        result.context  = le.getAttachment();
        ReusableBuffer buf = null;
        try {
            buf = le.serialize(checksum);
            result.data = buf.array();
            result.destinations = slaves;
            BufferPool.free(buf);
            checksum.reset();
            le.free();
        } catch (Exception e) {
            checksum.reset();
            if (buf!=null) BufferPool.free(buf);
            throw THIS.new PreProcessException("LogEntry could not be serialized because: "+e.getLocalizedMessage());
        }    
        
        assert (result.lsn!=null) : "BROADCAST misses a LSN.";
        assert (result.context!=null) : "BROADCAST misses the context.";
        assert (result.data!=null) : "BROADCAST misses the data.";
        assert (result.destinations!=null) : "BROADCAST misses destinations.";
        
        return result;
    }
    
    /**
     * <p>Parses the <code>error</code> and generates a LOAD_RQ if necessary.</p>
     * 
     * @param error - a DB failure.
     * @return a LOAD_RQ {@link Request}, if the DB structure is damaged, null otherwise.
     */
    static Request getReplicationRequest(BabuDBException error) {
        if (error!=null){
            if ((error.getErrorCode().equals(ErrorCode.NO_SUCH_DB) || 
                 error.getErrorCode().equals(ErrorCode.NO_SUCH_INDEX))){

                return new RequestImpl(LOAD_RQ);               
            }
        }
        return null;
    }
    
    /**
     * @return a state {@link Request}.
     */
    static Request getStateRequest(){
        return new RequestImpl(STATE);
    }
    
    /**
     * 
     * @return a new load {@link Request}.
     */
    static Request getLOAD_RQ(){
        return new RequestImpl(LOAD_RQ);
    }
    
    /**
     * 
     * @param lsn
     * @param destination
     * @return a new acknowledgment {@link Request}, which acknowledges the given {@link LSN} to the given destination.
     */
    static Request getACK_RQ(LSN lsn, InetSocketAddress destination) {
    	RequestImpl result = new RequestImpl(ACK_RQ,destination);
    	result.lsn = lsn;
    	return result;
    }
       
    /**
     * <b>WARNING: dangerous architecture break-through</b></br>
     * do not use this!
     * 
     * @return dummy LOAD_RP request for testing against the pending queue.
     */
    static Request getExpectedLOAD_RP() {
        return new RequestImpl(LOAD_RP);
    }
    
    /**
     * <b>WARNING: dangerous architecture break-through</b></br>
     * do not use this!
     * 
     * @param chunk
     * @return dummy CHUNK_RP request for testing against the pending queue.
     */
    static Request getExpectedCHUNK_RP(Chunk chunk) {
        RequestImpl result = new RequestImpl(CHUNK_RP);
        result.chunkDetails = chunk;
        return result;
    }
    
    /**
     * <b>WARNING: dangerous architecture break-through</b></br>
     * do not use this!
     * 
     * @param lsn
     * @return dummy REPLICA request for testing against the pending queue.
     */
    static Request getExpectedREPLICA(LSN lsn) {
        assert (lsn!=null) : "The LSN of an expected replica cannot be null!";
        
        RequestImpl result = new RequestImpl(REPLICA);
        result.lsn = lsn;
        return result;
    }
}
