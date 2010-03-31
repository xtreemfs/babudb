/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatRequest;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.MasterRequestDispatcher;
import org.xtreemfs.babudb.replication.SlavesStates.UnknownParticipantException;
import org.xtreemfs.foundation.TimeSync;

/**
 * {@link Operation} to send the {@link LSN} of the latest written {@link LogEntry} to the master.
 * 
 * @since 06/07/2009
 * @author flangner
 */

public class HeartbeatOperation extends Operation {

    private final int procId;
    
    private final MasterRequestDispatcher dispatcher;
    
    public HeartbeatOperation(MasterRequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new heartbeatRequest().getTag();
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
        heartbeatRequest rpcrq = new heartbeatRequest();
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
    public void startRequest(final Request rq) {
        heartbeatRequest request = (heartbeatRequest) rq.getRequestMessage();
        LSN lsn = new LSN(request.getLsn().getViewId(),request.getLsn().getSequenceNo());
        try {
            dispatcher.heartbeat(rq.getRPCRequest().getClientIdentity(), lsn, TimeSync.getLocalSystemTime());
            rq.sendSuccess(request.createDefaultResponse());
        } catch (UnknownParticipantException e) {
            rq.sendReplicationException(ErrNo.SECURITY,
                    "You are not allowed to request that!");
        } 
    }
}