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

import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
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
    
    /** table of available Operations */
    protected final Map<Integer, Operation>  operations = 
        new HashMap<Integer, Operation>();
    
    /** object to check if a requesting client is a replication participant */
    private final ParticipantsVerification   verificator;  
    
    public RequestHandler(ParticipantsVerification verificator) {
        this.verificator = verificator;
    }
        
    public abstract int getInterfaceID();
    
    public void handleRequest(RPCServerRequest rq) {
        
        if (!verificator.isRegistered(rq.getSenderAddress())){
            rq.sendError(ErrorType.AUTH_FAILED, POSIX_ERROR_NONE, "you " 
                    + rq.getSenderAddress().toString() + " have no access " 
                    + "rights to execute the requested operation");
            return;
        }
        
        final RequestHeader rqHdr = rq.getHeader().getRequestHeader();
        
        Operation op = operations.get(rqHdr.getProcId());
        if (op == null) {
            rq.sendError(ErrorType.INVALID_PROC_ID, POSIX_ERROR_NONE,
                    "requested operation (" + rqHdr.getProcId() + 
                    ") is not available");
            return;
        } 
        
        Request rpcrq = new Request(rq);
        try {
            Object message = op.parseRPCMessage(rpcrq);
            if (message != null) throw new Exception();
        } catch (Throwable ex) {
            rq.sendError(ErrorType.GARBAGE_ARGS, POSIX_ERROR_NONE, 
                    "message could not be parsed");
            return;
        }
        
        try {
            op.startRequest(rpcrq);
        } catch (Throwable ex) {
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            
            rq.sendError(ErrorType.INTERNAL_SERVER_ERROR, POSIX_ERROR_NONE, 
                    "internal server error: "+ex.toString(), 
                    OutputUtils.stackTraceToString(ex));
            return;
        }
    }
}
