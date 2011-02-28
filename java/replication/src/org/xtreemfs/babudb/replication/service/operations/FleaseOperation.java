/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.FLease;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;
import org.xtreemfs.foundation.util.OutputUtils;

import com.google.protobuf.Message;

/**
 * {@link Operation} to process an incoming {@link Flease} message.
 * 
 * @since 03/08/2010
 * @author flangner
 */

public class FleaseOperation extends Operation {
    
    private final FleaseMessageReceiver receiver;
    
    public FleaseOperation(FleaseMessageReceiver receiver) {
        this.receiver = receiver;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_FLEASE;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     * getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return FLease.getDefaultInstance();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     * parseRPCMessage(
     *          org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public ErrorResponse parseRPCMessage(Request rq) { 
        ErrorResponse resp = super.parseRPCMessage(rq);
        
        if (resp == null) {
            FleaseMessage message = new FleaseMessage(rq.getRpcRequest().getData());
            FLease rpcrq = (FLease) rq.getRequestMessage();
            assert (message != null);
            
            InetSocketAddress sender;
            try {
                sender = new InetSocketAddress(InetAddress.getByAddress(rpcrq.toByteArray()), 
                                                                        rpcrq.getPort());
            } catch (UnknownHostException e) {
                return ErrorResponse.newBuilder().setErrorMessage(e.getMessage())
                                                 .setErrorType(ErrorType.IO_ERROR)
                                                 .setDebugInfo(OutputUtils.stackTraceToString(e))
                                                 .build();
            }
            assert (sender != null);
            message.setSender(sender);
            
            rq.setAttachment(message);
        }
        
        return resp;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(final Request rq) {

        this.receiver.receive((FleaseMessage) rq.getAttachment());
        rq.sendSuccess(ErrorCodeResponse.getDefaultInstance());
    }
}