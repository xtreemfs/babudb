/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.deleteRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.deleteResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.TooBusyException;

import static org.xtreemfs.babudb.replication.stages.ReplicationStage.DELETE;

/**
 * {@link Operation} to replicate a delete-call from the master on a slave.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class DeleteOperation extends Operation {
    
    private final int procId;
    public final SlaveRequestDispatcher dispatcher;
    
    public DeleteOperation(SlaveRequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new deleteRequest().getOperationNumber();
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
        deleteRequest rpcrq = new deleteRequest();
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
        deleteRequest request = (deleteRequest) rq.getRequestMessage();
        LSN lsn = new LSN(request.getLsn().getViewId(),request.getLsn().getSequenceNo());
        try {
            dispatcher.replication.enqueueOperation(DELETE, new Object[]{ lsn, request.getDatabaseName(), request.getDeleteFiles() });
            rq.sendSuccess(new deleteResponse());
        } catch (TooBusyException e) {
            rq.sendReplicationException(ErrNo.TOO_BUSY.ordinal(), e.getMessage());
        }
    }
}