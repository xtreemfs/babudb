/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.babudb.interfaces.Exceptions.errnoException;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * Request object.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public class Request {
    
    private final ONCRPCRequest rpcRequest;
    
    private Serializable        requestMessage;
    
    private Object              attachment;
    
    public Request(ONCRPCRequest rpcRequest) {
        this.rpcRequest = rpcRequest;
    }

    public void deserializeMessage(Serializable message) {
        final ReusableBuffer payload = rpcRequest.getRequestFragment();
        message.deserialize(payload);
        requestMessage = message;
    }

    public Serializable getRequestMessage() {
        return requestMessage;
    }

    public void sendSuccess(Serializable response) {
        rpcRequest.sendResponse(response);
    }

    public void sendInternalServerError(Throwable rootCause) {
        rpcRequest.sendInternalServerError(rootCause);
    }

    public void sendException(ONCRPCException exception) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this,"sending exception return value: "+exception);
        }
        rpcRequest.sendGenericException(exception);
    }
    
    public void sendReplicationException(int errno, String message) {
        errnoException ex = new errnoException(errno, message, "");
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this,"sending errno exception "+ex);
        }
        getRPCRequest().sendGenericException(ex);
    }
    
    /**
     * @return the rpcRequest
     */
    public ONCRPCRequest getRPCRequest() {
        return rpcRequest;
    }
    
    /**
     * @param attachment to set
     */
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }
    
    /**
     * @return the attachment
     */
    public Object getAttachment() {
        return attachment;
    }
}
