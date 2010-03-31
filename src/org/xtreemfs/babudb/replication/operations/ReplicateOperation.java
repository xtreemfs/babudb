/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import java.net.InetSocketAddress;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.TooBusyException;
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
    private final Checksum checksum = new CRC32();     
    
    private final int procId;
    
    private final SlaveRequestDispatcher dispatcher;
    
    public ReplicateOperation(SlaveRequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new replicateRequest().getTag();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#getProcedureId()
     */
    @Override
    public int getProcedureId() {
        return procId;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#parseRPCMessage(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public yidl.runtime.Object parseRPCMessage(Request rq) {
        replicateRequest rpcrq = new replicateRequest();
        
        // check if requesting client is a master
        if (!(rq.getRPCRequest().getClientIdentity() instanceof InetSocketAddress) ||
            !((InetSocketAddress) rq.getRPCRequest().getClientIdentity())
            .getAddress().equals(dispatcher.master
            .getDefaultServerAddress().getAddress())) {
            Logging.logMessage(Logging.LEVEL_WARN, this, "The master (%s) was" +
            		" deprecated, expected was (%s)!", rq.getRPCRequest()
            		.getClientIdentity().toString(), dispatcher.master
            		.getDefaultServerAddress().toString());
            return rpcrq;
        }
        rq.deserializeMessage(rpcrq);
        
        ReusableBuffer data = rpcrq.getLogEntry().getPayload();
        try {
            rq.setAttachment(LogEntry.deserialize(data, checksum));
        } catch (LogEntryException e){
            Logging.logError(Logging.LEVEL_WARN, this, e);
            return rpcrq;
        } finally {
            checksum.reset();
            if (data!=null) BufferPool.free(data);
        } 
        
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#startInternalEvent(java.lang.Object[])
     */
    @Override
    public void startInternalEvent(Object[] args) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#startRequest(org.xtreemfs.babudb.replication.Request)
     */
    @Override
    public void startRequest(Request rq) {
        replicateRequest request = (replicateRequest) rq.getRequestMessage();
        final LSN lsn = new LSN(request.getLsn().getViewId(),
                request.getLsn().getSequenceNo());
        LogEntry le = (LogEntry) rq.getAttachment();
        try {
            dispatcher.replication.enqueueOperation(new Object[]{ lsn, le });
            rq.sendSuccess(request.createDefaultResponse());
        } catch (TooBusyException e) {
            if (le!=null) le.free();
            rq.sendReplicationException(ErrNo.TOO_BUSY,e.getMessage());
        }
    }
}