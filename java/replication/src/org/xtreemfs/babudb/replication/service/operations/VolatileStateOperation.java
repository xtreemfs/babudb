/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.logging.Logging;

import com.google.protobuf.Message;

/**
 * {@link Operation} to request the latest LSN at a {@link BabuDB} server.
 * 
 * @since 02/18/2011
 * @author flangner
 */

public class VolatileStateOperation extends Operation {
    
    private final BabuDBInterface dbInterface;
    
    public VolatileStateOperation(BabuDBInterface dbInterface) {
        this.dbInterface = dbInterface;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_VOLATILESTATE;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return LSN.getDefaultInstance();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#startRequest(
     *          org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        org.xtreemfs.babudb.lsmdb.LSN state = dbInterface.getState();
        Logging.logMessage(Logging.LEVEL_INFO, this, "StateOperation:" +
        		" reporting %s to %s.", state.toString(),
        		rq.getRPCRequest().getSenderAddress().toString());
        
        rq.sendSuccess(LSN.newBuilder().setViewId(state.getViewId())
                                       .setSequenceNo(state.getSequenceNo()).build());
    }
}