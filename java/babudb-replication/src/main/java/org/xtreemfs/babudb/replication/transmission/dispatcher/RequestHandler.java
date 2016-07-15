/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import static org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.POSIXErrno.POSIX_ERROR_NONE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.RequestHeader;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;
import org.xtreemfs.foundation.util.OutputUtils;

/**
 * Interface for objects handling RPC requests logically. There has to be one RequestHandler for 
 * each RPC interface available. Also capable to queue requests for a short time period 
 * (eg. failover).
 * 
 * @author flangner
 * @since 01/19/2011
 */
public abstract class RequestHandler implements RequestControl {
    
    /** table of available Operations */
    protected final Map<Integer, Operation>  operations = new HashMap<Integer, Operation>();
            
    /** flag to determine whether queuing of requests shall be permitted, or not */
    private final AtomicBoolean queuingEnabled = new AtomicBoolean(false);
    
    private final List<Request> queue = new ArrayList<Request>();
    
    private final int MAX_Q;
    
    public RequestHandler(int maxQ) {
        this.MAX_Q = maxQ;
    }
    
    /** method to identify the messages that have to be processed by this handler */
    public abstract int getInterfaceID();

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.RequestControl#enableQueuing()
     */
    @Override
    public void enableQueuing() {
        synchronized (queuingEnabled) {
            queuingEnabled.set(true);  
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.RequestControl#processQueue()
     */
    @Override
    public void processQueue() {
        
        List<Request> todo;
        synchronized (queuingEnabled) {
            queuingEnabled.set(false);
            todo = new ArrayList<Request>(queue);
            queue.clear();
        }
        
        for (Request req : todo) {
            if (!req.expired()) {
                req.getOperation().startRequest(req);
            }
        }
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
        
        Request rpcrq = new Request(rq, TimeSync.getGlobalTime(), op);
        ErrorResponse message = op.parseRPCMessage(rpcrq);
        if (message != null) {
            rq.sendError(message);
            return;
        } 
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "... parsed successfully ...");
        
        synchronized (queuingEnabled) {
            if (queuingEnabled.get()) {
                queue(rpcrq);
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
            }
        }
    }
    
    /**
     * Method to temporarily queue requests and execute them later.
     * 
     * @param rq
     */
    private void queue(Request rq) {
        
        // ensure that the queue does not growth beyond the MAX_Q limit by rejecting the oldest 
        // request queued
        if (queue.size() == MAX_Q) {
            Request req = queue.remove(0);
            if (!req.expired()) {
                req.sendError(ErrorType.INTERNAL_SERVER_ERROR, 
                        "Replication setup could not have been stabilized and " +
                        "servers run out of buffer.");
            }
        }
        
        queue.add(rq);
    }
}
