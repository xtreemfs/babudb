/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import java.io.IOException;

import org.xtreemfs.babudb.pbrpc.Common.emptyRequest;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;
import org.xtreemfs.foundation.util.OutputUtils;

import com.google.protobuf.Message;

/**
 * Super class for operations triggered by external {@link RPCServerRequest}s.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public abstract class Operation {
    
    /**
     * @return the unique operationId.
     */
    public abstract int getProcedureId();
    
    /**
     * Called after request was parsed and operation assigned. Before the logic of the request is 
     * processed it is tested for expiration.
     * 
     * @param rq the new request
     */
    public final void startRequest(Request rq) {
        
        if (!rq.expired()) {
            processRequest(rq);
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "... finished.");
    }
    
    /**
     * Logic for processing a request.
     * 
     * @param rq the new request
     */
    public abstract void processRequest(Request rq);
    
    /**
     * @return an empty message of the type of expected request message. may be
     *         null, if the {@link emptyRequest} is to be expected.
     */
    public abstract Message getDefaultRequest();
    
    /**
     * Parses the request.
     * 
     * @param rq the request
     * @return null if successful, error message otherwise.
     */
    public ErrorResponse parseRPCMessage(Request rq) {
        ErrorResponse result = null;
        try {
            rq.deserializeMessage(getDefaultRequest());
        } catch (IOException e) {
            result = ErrorResponse.newBuilder()
                .setErrorMessage(e.getMessage())
                .setErrorType(ErrorType.IO_ERROR)
                .setDebugInfo(OutputUtils.stackTraceToString(e)).build();
        }
        return result;
    }
}