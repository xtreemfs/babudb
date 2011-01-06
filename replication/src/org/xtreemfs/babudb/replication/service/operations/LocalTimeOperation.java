/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.Timestamp;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * {@link Operation} to answer a local-time request.
 * 
 * @since 03/30/2010
 * @author flangner
 */

public class LocalTimeOperation extends Operation {
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_LOCALTIME;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return null;
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
                rq.getRPCRequest().getSenderAddress().toString());
        
        rq.sendSuccess(Timestamp.newBuilder().setValue(time).build());
    }
}