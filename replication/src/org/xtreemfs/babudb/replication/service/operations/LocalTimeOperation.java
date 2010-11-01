/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.localTimeResponse;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;

/**
 * {@link Operation} to answer a local-time request.
 * 
 * @since 03/30/2010
 * @author flangner
 */

public class LocalTimeOperation extends Operation {

    private final int procId;
    
    public LocalTimeOperation() {
        this.procId = new localTimeRequest().getTag();
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
        localTimeRequest rpcrq = new localTimeRequest();
        rq.deserializeMessage(rpcrq);
        
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
        long time = TimeSync.getGlobalTime();
        Logging.logMessage(Logging.LEVEL_INFO, this, "LocalTimeOperation:" +
                " reporting %d to %s.", time,
                rq.getRPCRequest().getClientIdentity().toString());
        
        rq.sendSuccess(new localTimeResponse(time));
    }
}