/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import static org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.POSIXErrno.POSIX_ERROR_NONE;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.RequestHeader;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;
import org.xtreemfs.foundation.util.OutputUtils;

/**
 * Interface for objects handling RPC requests logically. There has to be one
 * RequestHandler for each RPC interface available.
 * 
 * @author flangner
 * @since 19.01.2011
 */
public abstract class RequestHandler {
    
    /** flag to determine whether queuing of requests shall be permitted, or not */
    protected final AtomicBoolean queuingEnabled = new AtomicBoolean(false);
    
    /** table of available Operations */
    protected final Map<Integer, Operation>  operations = new HashMap<Integer, Operation>();
            
    /** method to identify the messages that have to be processed by this handler */
    public abstract int getInterfaceID();
    
    /**
     * Method to temporarily queue requests and execute them later.
     * 
     * @param operationId
     * @param rq
     */
    public void queue(int operationId, Request rq) {
        throw new UnsupportedOperationException("This method has to be implemented by a " +
        		"superclass to support message queuing.");
    }
    
    public void handleRequest(RPCServerRequest rq) {
                
        final RequestHeader rqHdr = rq.getHeader().getRequestHeader();
        
        Operation op = operations.get(rqHdr.getProcId());
        if (op == null) {
            rq.sendError(ErrorType.INVALID_PROC_ID, POSIX_ERROR_NONE,
                    "requested operation (" + rqHdr.getProcId() + 
                    ") is not available");
            return;
        } 
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "... using operation %d ...", op.getProcedureId());
        
        Request rpcrq = new Request(rq);
        ErrorResponse message = op.parseRPCMessage(rpcrq);
        if (message != null) {
            rq.sendError(message);
            return;
        }
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "... parsed successfully ...");
        
        synchronized (queuingEnabled) {
            if (queuingEnabled.get()) {
                queue(op.getProcedureId(), rpcrq);
            } else {      
                
                try {
                    op.startRequest(rpcrq);
                } catch (Throwable ex) {
                    Logging.logError(Logging.LEVEL_ERROR, this, ex);
                    
                    rq.sendError(ErrorType.INTERNAL_SERVER_ERROR, POSIX_ERROR_NONE, 
                            "internal server error: " + ex.toString(), 
                            OutputUtils.stackTraceToString(ex));
                    return;
                }
                
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "... finished.");
            }
        }
    }
}
