/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.foundation.oncrpc.utils.XDRUnmarshaller;
import org.xtreemfs.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;

/**
 * Request object.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public class Request {
    
    private final ONCRPCRequest rpcRequest;
    
    private yidl.runtime.Object requestMessage;
    
    private Object              attachment;
    
    Request(ONCRPCRequest rpcRequest) {
        this.rpcRequest = rpcRequest;
    }

    public void deserializeMessage(yidl.runtime.Object message) {
        final ReusableBuffer payload = rpcRequest.getRequestFragment();
        message.unmarshal(new XDRUnmarshaller(payload));
        requestMessage = message;
    }

    public yidl.runtime.Object getRequestMessage() {
        return requestMessage;
    }

    public void sendSuccess(yidl.runtime.Object response) {
        rpcRequest.sendResponse(response);
    }

    public void sendException(ONCRPCException exception) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this,"sending exception return value: "+exception);
        }
        rpcRequest.sendException(exception);
    }
    
    public void sendReplicationException(int errno, String message) {
        if (Logging.isDebug()) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this,"sending errno exception #"+errno);
        }
        getRPCRequest().sendException(new errnoException(errno, message, null));
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
