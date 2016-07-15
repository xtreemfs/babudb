/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;

import com.google.protobuf.Message;

/**
 * {@link Operation} to request the latest LSN at a {@link BabuDB} server. And preserve this
 * state until a master failover has finished.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class StateOperation extends Operation {
    
    private final BabuDBInterface               dbInterface;
    private final ControlLayerInterface         topLayer;
    
    public StateOperation(BabuDBInterface dbInterface, ControlLayerInterface topLayer) {
        this.dbInterface = dbInterface;
        this.topLayer = topLayer;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_STATE;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return LSN.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(Request rq) {
        
        try {
            
            topLayer.lockReplication();
            org.xtreemfs.babudb.lsmdb.LSN state = dbInterface.getState();
            Logging.logMessage(Logging.LEVEL_INFO, this, "StateOperation:" +
                        " reporting %s to %s.", state.toString(),
                        rq.getSenderAddress().toString());
        
            rq.sendSuccess(LSN.newBuilder().setViewId(state.getViewId())
                                           .setSequenceNo(state.getSequenceNo()).build());
    
        } catch (InterruptedException e) {
            
            Logging.logError(Logging.LEVEL_WARN, this, e);
            rq.sendError(ErrorType.INTERNAL_SERVER_ERROR, "Server could not establish a stable " +
                        "state, because: " + e.getMessage());
        } finally {
            topLayer.unlockReplication();
        }
    }
}