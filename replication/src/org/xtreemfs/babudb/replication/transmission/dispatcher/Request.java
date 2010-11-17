/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import java.io.IOException;

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
 * Request object.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public class Request {
    
    private final RPCServerRequest      rpcRequest;
    
    private Message                     requestMessage;
    
    private static long rqIdCounter = 1;  
    private long                        requestId;
    
    private Object                      attachment;
    
    /**
     * Request operation which contains state machine.
     */
    private Operation                   replicationOperation;
    
    
    
    Request(RPCServerRequest rpcRequest) {
        this.rpcRequest = rpcRequest;
        this.requestId = rqIdCounter++;
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
        if (getRpcRequest() != null) {
            rpcRequest.sendError(ErrorType.INTERNAL_SERVER_ERROR, 
                    POSIXErrno.POSIX_ERROR_NONE, "internal server error:" + 
                    cause, OutputUtils.stackTraceToString(cause));
        } else {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "internal server " 
                    + "error on internal request: %s", cause.toString());
            Logging.logError(Logging.LEVEL_ERROR, this, cause);
        }
    }
    
    public void sendReplicationException(int errno, String message) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, Category.stage, this,
                    "sending errno exception #"+errno);
        }
        rpcRequest.sendError(ErrorType.IO_ERROR, POSIXErrno.POSIX_ERROR_NONE, 
                message);
    }
    
    public void sendReplicationException(int errno, String message, Throwable t) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, Category.stage, this,
                    "sending errno exception #"+errno);
        }
        rpcRequest.sendError(ErrorType.IO_ERROR, POSIXErrno.POSIX_ERROR_NONE, 
                message, OutputUtils.stackTraceToString(t));
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
        this.getRPCRequest().sendError(error);
    }
    
    public RPCServerRequest getRPCRequest() {
        return this.getRpcRequest();
    }

    /**
     * @return the requestId
     */
    public long getRequestId() {
        return requestId;
    }
    
    /**
     * @return the rpcRequest
     */
    public RPCServerRequest getRpcRequest() {
        return rpcRequest;
    }

    /**
     * @return the requestMessages
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
     * @param operation the operation to set
     */
    public void setOperation(Operation operation) {
        this.replicationOperation = operation;
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
}
