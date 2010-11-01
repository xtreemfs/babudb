/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.fleaseResponse;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 * {@link Operation} to process an incoming {@link Flease} message.
 * 
 * @since 03/08/2010
 * @author flangner
 */

public class FleaseOperation extends Operation {

    private final int                   procId;
    
    private final FleaseMessageReceiver receiver;
    
    public FleaseOperation(FleaseMessageReceiver receiver) {
        this.receiver = receiver;
        this.procId = new fleaseRequest().getTag();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return this.procId;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#parseRPCMessage(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public yidl.runtime.Object parseRPCMessage(final Request rq) {
        fleaseRequest rpcrq = new fleaseRequest();
        rq.deserializeMessage(rpcrq);
        
        FleaseMessage message = new FleaseMessage(rpcrq.getMessage());
        assert (message != null);
        
        InetSocketAddress sender = new InetSocketAddress(rpcrq.getHost(), rpcrq.getPort());
        assert (sender != null);
        message.setSender(sender);
        
        rq.setAttachment(message);
        BufferPool.free(rpcrq.getMessage());
        
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#startInternalEvent(java.lang.Object[])
     */
    @Override
    public void startInternalEvent(Object[] args) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(final Request rq) {
        this.receiver.receive((FleaseMessage) rq.getAttachment());
        
        rq.sendSuccess(new fleaseResponse());
    }
}