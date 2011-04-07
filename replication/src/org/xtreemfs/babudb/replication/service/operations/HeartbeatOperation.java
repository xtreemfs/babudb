/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.HeartbeatMessage;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.service.accounting.StatesManipulation;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;

import com.google.protobuf.Message;

/**
 * <p>
 * {@link Operation} to send the {@link LSN} of the latest written 
 * {@link LogEntry} to the master.
 * </p>
 * 
 * @since 06/07/2009
 * @author flangner
 */

public class HeartbeatOperation extends Operation {
    
    private final StatesManipulation    sManipulator;
    
    public HeartbeatOperation(StatesManipulation statesManipulator) {
        this.sManipulator = statesManipulator;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_HEARTBEAT;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     * getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return HeartbeatMessage.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     *          processRequest(org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void processRequest(Request rq) {
        
        HeartbeatMessage message = (HeartbeatMessage) rq.getRequestMessage();
        LSN rawLSN = message.getLsn();
        org.xtreemfs.babudb.lsmdb.LSN lsn = new org.xtreemfs.babudb.lsmdb.LSN(rawLSN.getViewId(), 
                                                        rawLSN.getSequenceNo());
        try {
            
            InetSocketAddress participant = new InetSocketAddress(
                    rq.getSenderAddress().getAddress(), message.getPort());
            
            sManipulator.update(participant, lsn, TimeSync.getGlobalTime());
            
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "HeartbeatOperation:" +
                    " received %s from %s.", lsn.toString(), participant);
            
            rq.sendSuccess(ErrorCodeResponse.getDefaultInstance());
        } catch (UnknownParticipantException e) {
            
            rq.sendError(ErrorResponse.newBuilder()
                    .setErrorMessage("The sender address of the received " 
                            + "request did not match any expected address.")
                    .setErrorType(ErrorType.AUTH_FAILED).build());
        } 
    }
}