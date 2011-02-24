/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.net.InetSocketAddress;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.service.RequestManagement;
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
                
    public ReplicateOperation(RequestManagement rqMan) {
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
            
        // dezerialize message
        if (resp == null) {
            resp = super.parseRPCMessage(rq);
        }
        
        //dezerialize payload (serialized logEntry)
        if (resp == null) {
            
            ReusableBuffer data = rq.getRpcRequest().getData().createViewBuffer();
            try {
                rq.setAttachment(LogEntry.deserialize(data, checksum));
            } catch (LogEntryException e){
                Logging.logError(Logging.LEVEL_WARN, this, e);
                resp = ErrorResponse.newBuilder()
                        .setErrorMessage(e.getMessage())
                        .setErrorType(ErrorType.IO_ERROR).build();
            } finally {
                checksum.reset();
                if (data != null) BufferPool.free(data);
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
            // the master requests to synchronize this server to the last stable state
            if (le.getPayload().remaining() == 0) {
                rqMan.createStableState(lsn, 
                        ((InetSocketAddress) rq.getRpcRequest().getSenderAddress()).getAddress());
                
            // this is an ordinary replicate request    
            } else {
                rqMan.enqueueOperation(new Object[]{ lsn, le });
            }
            rq.sendSuccess(ErrorCodeResponse.getDefaultInstance());
        } catch (Exception e) {
            if (le!=null) le.free();
            rq.sendSuccess(ErrorCodeResponse.newBuilder().setErrorCode(ErrorCode.BUSY).build());
        }
    }
}