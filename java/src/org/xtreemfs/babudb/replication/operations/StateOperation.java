/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import org.xtreemfs.babudb.interfaces.LSN;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.stateResponse;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.foundation.logging.Logging;

/**
 * {@link Operation} to request the latest {@link org.xtreemfs.babudb.lsmdb.LSN} 
 * on a {@link BabuDB} server.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class StateOperation extends Operation {

    private final int procId;
    
    private final RequestDispatcher dispatcher;
    
    public StateOperation(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new stateRequest().getTag();
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
    public yidl.runtime.Object parseRPCMessage(Request rq) {
        stateRequest rpcrq = new stateRequest();
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
        DispatcherState state = dispatcher.getState(); 
        Logging.logMessage(Logging.LEVEL_INFO, this, "StateOperation:" +
        		" reporting %s to %s.", state.latest.toString(),
        		rq.getRPCRequest().getClientIdentity().toString());
        
        rq.sendSuccess(
                new stateResponse(
                        new LSN(
                                state.latest.getViewId(),
                                state.latest.getSequenceNo()
                                )
                        )
                );
    }
}