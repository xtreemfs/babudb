/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.SimplifiedBabuDBRequestListener;
import org.xtreemfs.babudb.interfaces.LSN;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.remoteStopRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.remoteStopResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;

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
        // stop the replication
        final AtomicBoolean ready = new AtomicBoolean(false);
        
        dispatcher.pauses(new SimplifiedBabuDBRequestListener() {
        
            @Override
            public void finished(BabuDBException error) {
                synchronized (ready) {
                    ready.set(true);
                    ready.notify();
                }
            }
        });
        
        synchronized (ready) {
            try {
                if (!ready.get())
                    ready.wait();
            } catch (InterruptedException ie) {
                rq.sendReplicationException(ErrNo.INTERNAL_ERROR, ie.getMessage());
                return;
            }
        }
        
        // stop the babuDB
        DispatcherState state = dispatcher.getState();
        LSN result = new LSN(state.latest.getViewId(),state.latest.getSequenceNo());
        rq.sendSuccess(new remoteStopResponse(result));
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