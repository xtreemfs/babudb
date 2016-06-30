/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.util.OutputUtils;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.POSIXErrno;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;
import org.xtreemfs.foundation.pbrpc.utils.ReusableBufferInputStream;

import com.google.protobuf.Message;

/**
 * Request object. Access is not thread-safe.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public class Request implements Comparable<Request> {
    
    private static long                 rqIdCounter = 1;  
    
    private final RPCServerRequest      rpcRequest;
    
    private Message                     requestMessage;
    
    private long                        requestId;
    
    private Object                      attachment;
    
    /**
     * Request operation which contains state machine.
     */
    private final Operation             replicationOperation;
    
    private final long                  timestamp;
    
    private boolean                     expired;
    
    Request(RPCServerRequest rpcRequest, long timestamp, Operation operation) {
        this.rpcRequest = rpcRequest;
        this.requestId = rqIdCounter++;
        this.timestamp = timestamp;
        this.replicationOperation = operation;
    }
    
    public void deserializeMessage(Message message) throws IOException {
        final ReusableBuffer payload = rpcRequest.getMessage();
        if (message != null) {
            if (payload != null) {
                ReusableBufferInputStream istream = new ReusableBufferInputStream(payload);
                requestMessage = message.newBuilderForType().mergeFrom(istream).build();
                if (Logging.isDebug()) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "parsed request: %s",requestMessage.toString());
                }
            } else {
                requestMessage = message.getDefaultInstanceForType();
            }
        } else {
            requestMessage = null;
            if (Logging.isDebug()) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "parsed request: empty message (emptyRequest)");
            }
        }
    }
       
    public void sendSuccess(Message response) {
        sendSuccess(response, null);
    }
    
    public void sendSuccess(Message response, ReusableBuffer data) {
        try {
            rpcRequest.sendResponse(response, data);
        } catch (IOException ex) {
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
        }
    }

    public void sendInternalServerError(Throwable cause) {
        if (rpcRequest != null) {
            rpcRequest.sendError(ErrorType.INTERNAL_SERVER_ERROR, 
                    POSIXErrno.POSIX_ERROR_NONE, "internal server error:" + 
                    cause, OutputUtils.stackTraceToString(cause));
        } else {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "internal server " 
                    + "error on internal request: %s", cause.toString());
            Logging.logError(Logging.LEVEL_ERROR, this, cause);
        }
    }

    public void sendError(ErrorType type, String message) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, Category.stage, this, 
                    "sending errno exception %s/%s", type, message);
        }
        rpcRequest.sendError(type, POSIXErrno.POSIX_ERROR_NONE, message);
    }

    public void sendError(ErrorType type, String message,  String debugInfo) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, Category.stage, this, 
                    "sending errno exception %s/%s/%s", type, message, 
                    debugInfo);
        }
        rpcRequest.sendError(type, POSIXErrno.POSIX_ERROR_NONE, message, 
                debugInfo);
    }
    
    public void sendError(ErrorResponse error) {
        rpcRequest.sendError(error);
    }
    
//    public RPCServerRequest getRPCRequest() {
//        return this.getRpcRequest();
//    }

    /**
     * @return the requestId
     */
    public long getRequestId() {
        return requestId;
    }
    
//    /**
//     * @return the rpcRequest
//     */
//    public RPCServerRequest getRpcRequest() {
//        return rpcRequest;
//    }

    /**
     * @return the data attached to the request.
     */
    public ReusableBuffer getData() {
        return rpcRequest.getData();
    }
    
    public InetSocketAddress getSenderAddress() {
        return (InetSocketAddress) rpcRequest.getSenderAddress();
    }
    
    /**
     * @return the requestMessage.
     */
    public Message getRequestMessage() {
        return requestMessage;
    }
    
    /**
     * @return the operation
     */
    public Operation getOperation() {
        return replicationOperation;
    }

    /**
     * @return the attachment
     */
    public Object getAttachment() {
        return attachment;
    }
    
    /**
     * @param attachment the attachment to set
     */
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }
    
    /**
     * @return true if the request has been expired.
     */
    public boolean expired() {
        if (expired) {
            return true;
        } else if ((timestamp + ReplicationConfig.REQUEST_TIMEOUT) < TimeSync.getGlobalTime()) {
            free();
            expired = true;
            return true;
        }
        
        return false;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Request o) {
        return (int) (timestamp - o.timestamp);
    }
    
    /**
     * Frees the rpcRequest and all data stored at the attachment.
     */
    private final void free() {
        rpcRequest.freeBuffers();
        if (attachment instanceof ReusableBuffer) {
            BufferPool.free((ReusableBuffer) attachment);
        } else if (attachment instanceof LogEntry) {
            ((LogEntry) attachment).free();
        }
    }
}
