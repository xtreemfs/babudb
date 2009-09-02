/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toSlaveRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toSlaveResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.include.common.logging.Logging;

/**
 * {@link Operation} to request the latest {@link org.xtreemfs.babudb.lsmdb.LSN} on a list of {@link BabuDB}s.
 * 
 * @since 08/31/2009
 * @author flangner
 */

public class ToSlaveOperation extends Operation {

    private final int procId;
    
    private final RequestDispatcher dispatcher;
    
    public ToSlaveOperation(RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new toSlaveRequest().getTag();
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
        toSlaveRequest rpcrq = new toSlaveRequest();
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
        try {
            org.xtreemfs.babudb.lsmdb.LSN lsn = dispatcher.dbs.restart();
            lsn.getSequenceNo();
            // TODO restart of the replication
            
            rq.sendSuccess(new toSlaveResponse());
        } catch (BabuDBException be) {
            Logging.logError(Logging.LEVEL_ERROR, this, be);
            rq.sendReplicationException(ErrNo.INTERNAL_ERROR);
        }
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