/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.RequestManagement;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.babudb.replication.transmission.dispatcher.ErrNo;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * {@link Operation} to replicate a {@link LogEntry} from the master on a slave.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ReplicateOperation extends Operation {

    /** Object for generating check sums */
    private final Checksum                      checksum = new CRC32();     
    
    private final int                           procId;
    
    private final RequestManagement             rqMan;
    
    private final ParticipantsVerification      verificator;
        
    public ReplicateOperation(RequestManagement rqMan, 
            ParticipantsVerification verificator) {
        this.verificator = verificator;
        this.rqMan = rqMan;
        this.procId = new replicateRequest().getTag();
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
    public yidl.runtime.Object parseRPCMessage(Request rq) {
        replicateRequest rpcrq = new replicateRequest();
        
        // check if requesting client is a master
        if (!verificator.isMaster(rq.getRPCRequest().getClientIdentity())) {
            Logging.logMessage(Logging.LEVEL_WARN, this, 
                    "The master (%s) was deprecated!", 
                    rq.getRPCRequest().getClientIdentity().toString());
            return rpcrq;
        }

        rq.deserializeMessage(rpcrq);
        
        ReusableBuffer data = rpcrq.getLogEntry().getPayload();
        try {
            rq.setAttachment(LogEntry.deserialize(data, this.checksum));
        } catch (LogEntryException e){
            Logging.logError(Logging.LEVEL_WARN, this, e);
            return rpcrq;
        } finally {
            this.checksum.reset();
            if (data!=null) BufferPool.free(data);
        } 
        
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
    public void startRequest(Request rq) {
        replicateRequest request = (replicateRequest) rq.getRequestMessage();
        final LSN lsn = new LSN(request.getLsn());
        
        LogEntry le = (LogEntry) rq.getAttachment();
        try {
            this.rqMan.enqueueOperation(new Object[]{ lsn, le });
            rq.sendSuccess(request.createDefaultResponse());
        } catch (BusyServerException e) {
            if (le!=null) le.free();
            rq.sendReplicationException(ErrNo.BUSY,e.getMessage());
        }
    }
}