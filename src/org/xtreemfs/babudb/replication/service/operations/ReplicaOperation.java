/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaResponse;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.ErrNo;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.logging.Logging;

/**
 * {@link Operation} to request {@link LogEntry}s from the master.
 * This operation tries to retrieve the logEntries from the log by iterating
 * over them and returning the requested ones.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ReplicaOperation extends Operation {

    private final static int            MAX_LOGENTRIES_PER_REQUEST = 500;
    
    private final int                   procId;
        
    private final Checksum              checksum = new CRC32();
    
    private final AtomicReference<LSN>  lastOnView;
    
    private final BabuDBInterface       babuInterface;
    
    private final FileIOInterface       fileIO;
    
    public ReplicaOperation(AtomicReference<LSN> lastOnView, 
            BabuDBInterface babuInterface, FileIOInterface fileIO) {
        
        this.fileIO = fileIO;
        this.babuInterface = babuInterface;
        this.lastOnView = lastOnView;
        this.procId = new replicaRequest().getTag();
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
        replicaRequest rpcrq = new replicaRequest();
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
    public void startRequest(Request rq) {
        replicaRequest req = (replicaRequest) rq.getRequestMessage();
        
        final LSN start = new LSN(req.getRange().getStart());
        final LSN end = new LSN(req.getRange().getEnd());
        
        LogEntries result = new LogEntries();
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "REQUEST received " +
                "(start: %s, end: %s) from %s", start.toString(), 
                end.toString(), rq.getRPCRequest().getClientIdentity());
        
        // enhancement to prevent slaves from loading the db from the master un-
        // necessarily
        if (start.equals(lastOnView.get())) {
            
            Logging.logMessage(Logging.LEVEL_INFO, this, 
                   "REQUEST answer is empty (there has been a failover only).");
            
            rq.sendSuccess(new replicaResponse(result));
            return;  
        }
        
        final LSN firstEntry = new LSN(start.getViewId(), 
                start.getSequenceNo() + 1L);
        
        assert (firstEntry.compareTo(end) < 0) : 
            "At least one LogEntry has to be requested!";
        
        DiskLogIterator it = null;
        LogEntry le = null;
        synchronized (babuInterface.getCheckpointerLock()) {
            try {   
                // wait, if there is a checkpoint in proceeding
                babuInterface.waitForCheckpoint();
                
                it = fileIO.getLogEntryIterator(start);
                
                while (it.hasNext() &&
                       result.size() < MAX_LOGENTRIES_PER_REQUEST &&
                       (le = it.next()).getLSN().compareTo(end) < 0) {
                    
                    try {
                        // we are not at the right position yet -> skip
                        if (le.getLSN().compareTo(firstEntry) < 0) continue;
                    
                        // the first entry was not available ... bad
                        if (le.getLSN().compareTo(firstEntry) > 0 &&
                            result.size() == 0) {
                            break;
                        }
                          
                        // add the logEntry to result list
                        assert (le.getPayload().array().length > 0) : 
                            "Empty log-entries are not allowed!";
                        result.add(new org.xtreemfs.babudb.interfaces.LogEntry(
                                le.serialize(this.checksum)));
                    } finally {
                        this.checksum.reset();
                        if (le != null) {
                            le.free();
                            le = null;
                        }
                    }
                } 
                
                if (result.size() > 0) {
                    // send the response, if the requested log entries are found
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                            "REQUEST: returning %d log-entries to %s.", 
                            result.size(), rq.getRPCRequest().getClientIdentity());
                    
                    rq.sendSuccess(new replicaResponse(result));
                } else {
                    rq.sendReplicationException(ErrNo.LOG_UNAVAILABLE,
                            "No entries left within the log files, and " +
                            "there was no entry added yet.");
                }
            } catch (Exception e) {
                Logging.logError(Logging.LEVEL_INFO, this, e);
                
                // clear result
                for (org.xtreemfs.babudb.interfaces.LogEntry entry : result) {
                    BufferPool.free(entry.getPayload());
                }
                
                rq.sendReplicationException(ErrNo.BUSY,
                        "Request not finished: "+e.getMessage(), e);
            } finally {
                if (le != null) le.free();
                
                if (it != null) {
                    try {
                        it.destroy();
                    } catch (IOException e) { /* ignored */ }
                }
            }
        }
    }
}