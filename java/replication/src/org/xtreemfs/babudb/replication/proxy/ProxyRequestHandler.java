/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabaseByIdOperation;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabaseByNameOperation;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabasesOperation;
import org.xtreemfs.babudb.replication.proxy.operations.LookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.MakePersistentOperation;
import org.xtreemfs.babudb.replication.proxy.operations.PrefixLookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.PrefixLookupReverseOperation;
import org.xtreemfs.babudb.replication.proxy.operations.RangeLookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.RangeLookupReverseOperation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.POSIXErrno;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;
import org.xtreemfs.foundation.pbrpc.server.RPCServerRequest;

/**
 * Organizes the logic to dispatch requests matching the RemoteAccessInterface logically.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class ProxyRequestHandler extends RequestHandler {
    
    private final ControlLayerInterface control;
    
    /**
     * @param maxQ
     * @param dbs - interface for local BabuDB operations.
     */
    public ProxyRequestHandler(BabuDBInterface dbs, int maxQ, ControlLayerInterface control) {
        super(maxQ);
        
        this.control = control;
        
        Operation op = new MakePersistentOperation(dbs); 
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabaseByNameOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabaseByIdOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabasesOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new LookupOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new PrefixLookupOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new PrefixLookupReverseOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new RangeLookupOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new RangeLookupReverseOperation(dbs);
        operations.put(op.getProcedureId(), op);
    }
    
    @Override
    public void handleRequest(RPCServerRequest rq) {
        if (control.isFailoverInProgress()) {
            super.handleRequest(rq);
        } else {
            rq.sendError(ErrorResponse.newBuilder()
                    .setErrorMessage("Currently there is a failover in progress. Please try again later.")
                    .setErrorType(ErrorType.REDIRECT)
                    .setPosixErrno(POSIXErrno.POSIX_ERROR_NONE).build());
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#getInterfaceID()
     */
    @Override
    public int getInterfaceID() {
        return RemoteAccessServiceConstants.INTERFACE_ID;
    }
}
