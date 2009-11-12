/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.interfaces.LSN;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.remoteStopRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.remoteStopResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.logging.Logging;

/**
 * {@link Operation} to stop an instance of {@link BabuDB} remotely.
 * 
 * @since 08/31/2009
 * @author flangner
 */

public class RemoteStopOperation extends Operation {

    private final int procId;
    
    private final RequestDispatcher dispatcher;
    
    public RemoteStopOperation(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new remoteStopRequest().getTag();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return procId;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#parseRPCMessage(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public Serializable parseRPCMessage(Request rq) {
        remoteStopRequest rpcrq = new remoteStopRequest();
        rq.deserializeMessage(rpcrq);
                
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#startInternalEvent(java.lang.Object[])
     */
    @Override
    public void startInternalEvent(Object[] args) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Stopped by: %s", 
                rq.getRPCRequest().getClientIdentity().toString());
        
        // stop the replication
        BabuDBRequest<Object> ready = new BabuDBRequest<Object>();
        
        // wait for the request-queue to run empty
        dispatcher.pauses(ready);
        
        try {
            ready.get();
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_WARN, this, e);
            rq.sendReplicationException(ErrNo.INTERNAL_ERROR, e.getMessage());
            return;
        }
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Replication: %s", "stopped.");
        
        // wait for the DiskLogger to finish the requests
        ready.recycle();
        dispatcher.dbs.getLogger().registerListener(ready);
        
        try {
            ready.get();
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_WARN, this, e);
            rq.sendReplicationException(ErrNo.INTERNAL_ERROR, e.getMessage());
            return;
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Logger: %s", "stopped.");
        
        // stop the babuDB
        DispatcherState state = dispatcher.getState();
        Logging.logMessage(Logging.LEVEL_INFO, this, 
                "Remotely stopped at state: %s", state.toString());
        
        // send the answer
        rq.sendSuccess(new remoteStopResponse(new LSN(state.latest.getViewId(), 
                state.latest.getSequenceNo())));
        
        // discard the remaining requests 
        if (state.requestQueue != null)
            for (StageRequest request : state.requestQueue)
                request.free();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#canBeDisabled()
     */
    @Override
    public boolean canBeDisabled() {
        return false;
    }
}