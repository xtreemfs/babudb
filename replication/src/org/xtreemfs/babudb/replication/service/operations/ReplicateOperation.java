/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.service.RequestManagement;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.RPCHeader.ErrorResponse;

import com.google.protobuf.Message;

/**
 * {@link Operation} to replicate a {@link LogEntry} from the master on a slave.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ReplicateOperation extends Operation {

    /** Object for generating check sums */
    private final Checksum                      checksum = new CRC32();     
        
    private final RequestManagement             rqMan;
    
    private final ParticipantsVerification      verificator;
        
    public ReplicateOperation(RequestManagement rqMan, 
            ParticipantsVerification verificator) {
        
        this.verificator = verificator;
        this.rqMan = rqMan;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return ReplicationServiceConstants.PROC_ID_REPLICATE;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     * getDefaultRequest()
     */
    @Override
    public Message getDefaultRequest() {
        return org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN.getDefaultInstance();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.Operation#
     * parseRPCMessage(
     *          org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public ErrorResponse parseRPCMessage(Request rq) {
        ErrorResponse resp = null;
        
        // check if requesting client is a master
        if (!verificator.isMaster(rq.getRPCRequest().getSenderAddress())) {
            Logging.logMessage(Logging.LEVEL_WARN, this, 
                    "The master (%s) was deprecated!", 
                    rq.getRPCRequest().getSenderAddress().toString());
            
            resp = ErrorResponse.newBuilder()
                    .setErrorMessage("The sender address of the received " 
                            + "request did not match the expected master " 
                            + "address.")
                    .setErrorType(ErrorType.AUTH_FAILED).build();
        }
            
        // dezerialize message
        if (resp == null) {
            resp = super.parseRPCMessage(rq);
        }
        
        //dezerialize payload (serialized logEntry)
        if (resp == null) {
            
            ReusableBuffer data = rq.getRpcRequest().getData();
            try {
                rq.setAttachment(LogEntry.deserialize(data, this.checksum));
            } catch (LogEntryException e){
                Logging.logError(Logging.LEVEL_WARN, this, e);
                resp = ErrorResponse.newBuilder()
                        .setErrorMessage(e.getMessage())
                        .setErrorType(ErrorType.IO_ERROR).build();
            } finally {
                this.checksum.reset();
                if (data!=null) BufferPool.free(data);
            } 
        }
        return resp;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.operations.Operation#
     * startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        
        LogEntry le = (LogEntry) rq.getAttachment();
        final LSN lsn = le.getLSN();
        try {
            this.rqMan.enqueueOperation(new Object[]{ lsn, le });
            rq.sendSuccess(ErrorCodeResponse.getDefaultInstance());
        } catch (BusyServerException e) {
            if (le!=null) le.free();
            rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(
                    ErrorCode.BUSY).build());
        }
    }
}