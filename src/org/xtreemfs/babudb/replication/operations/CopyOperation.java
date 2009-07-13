/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.copyRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.copyResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.TooBusyException;

import static org.xtreemfs.babudb.replication.stages.ReplicationStage.COPY;

/**
 * {@link Operation} to replicate a copy-call from the master on a slave.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class CopyOperation extends Operation {

    private final int procId;
    private final SlaveRequestDispatcher dispatcher;
    
    public CopyOperation(SlaveRequestDispatcher dispatcher) {
        procId = new copyRequest().getOperationNumber();
        this.dispatcher = dispatcher;
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
        copyRequest rpcrq = new copyRequest();
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
        copyRequest request = (copyRequest) rq.getRequestMessage();
        LSN lsn = new LSN(request.getLsn().getViewId(),request.getLsn().getSequenceNo());
        try {
            dispatcher.replication.enqueueOperation(COPY, new Object[]{ lsn, request.getSourceDB(), request.getDestDB() });
            rq.sendSuccess(new copyResponse());
        } catch (TooBusyException e) {
            rq.sendReplicationException(ErrNo.TOO_BUSY.ordinal(), e.getMessage());
        }
    }
}