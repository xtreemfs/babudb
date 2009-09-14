/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.toSlaveRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.toSlaveResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.replication.ReplicationManagerImpl;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;

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
        toSlaveRequest request = (toSlaveRequest) rq.getRequestMessage();
        
        if (!dispatcher.stopped) rq.sendReplicationException(ErrNo.
                SERVICE_UNAVAILABLE, "Replication is running at the moment!");
        else {
            InetSocketAddress masterAddress = new InetSocketAddress(request.
                    getAddress().getAddress(),request.getAddress().getPort());
            
            SlaveRequestDispatcher newDispatcher = new SlaveRequestDispatcher(
                    dispatcher);
            
            ((ReplicationManagerImpl) dispatcher.dbs.getReplicationManager()).
                        renewDispatcher(newDispatcher);
            newDispatcher.coin(masterAddress);
            rq.sendSuccess(new toSlaveResponse());
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