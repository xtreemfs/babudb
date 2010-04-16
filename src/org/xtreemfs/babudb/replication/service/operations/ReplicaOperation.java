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
        
        LSN start = new LSN(req.getRange().getStart());
        LSN end = new LSN(req.getRange().getEnd());
       
        LogEntries result = new LogEntries();
        
        boolean deadEnd = (end.getSequenceNo() == 0L);
        
        // enhancement to prevent slaves from loading the db from the master un-
        // necessarily
        boolean enhancedStart;
        LSN lastOnView = this.lastOnView.get();
        if (start.equals(lastOnView) && 
            start.getViewId()+1 == end.getViewId() && 
            end.getSequenceNo() == 0L) {
            rq.sendSuccess(new replicaResponse(result));
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "REQUEST answered " +
                    "(start: %s, end: %s) from %s ... with null.", 
                    start.toString(), end.toString(),
                    rq.getRPCRequest().getClientIdentity());
            return;
            
        } else if (enhancedStart = (start.equals(lastOnView) && !deadEnd)) {
            
            start = new LSN(start.getViewId()+1, 1L);
        } else if (enhancedStart = (start.getSequenceNo() == 0L)) {
            
            start = new LSN(start.getViewId(), 1L);
        }
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "REQUEST received " +
                "(start: %s, end: %s) from %s", start.toString(), 
                end.toString(), rq.getRPCRequest().getClientIdentity());
        
        assert (start.compareTo(end) < 0 || 
               (enhancedStart && start.compareTo(end) <= 0)) : 
            "Always request at least one LogEntry!";
        
        LogEntry le = null;
        DiskLogIterator it = null;
        
        synchronized (babuInterface.getCheckpointerLock()) {
            try {   
                babuInterface.waitForCheckpoint();
                
                it = new DiskLogIterator(fileIO.getLogFiles(), start);
                
                // discard the first entry, because it was the last inserted
                // entry of the requesting server
                if (!enhancedStart) it.next().free();
                
                int counter = 0;
                do {
                    try {
                        if (!it.hasNext()) {
                            if (result.size() > 0) {
                                break;
                            } else {
                                rq.sendReplicationException(ErrNo.LOG_REMOVED,
                                "LogEntry unavailable.");
                                return;
                            }
                        }
                        le = it.next();
                        // we are not at the right position yet
                        if (le.getLSN().compareTo(start) < 0) continue;
                        
                        // the start entry is not available ... bad
                        if (le.getLSN().compareTo(start) > 0 &&
                            counter == 0) {
                            rq.sendReplicationException(ErrNo.LOG_REMOVED,
                                    "LogEntry unavailable.");
                                    return;
                        }
                        
                        // we skip the last entry, because the client already
                        // has it
                        if (deadEnd && le.getLSN().compareTo(end) > 0) break;
                        
                        assert (le.getPayload().array().length > 0) : 
                            "Empty log-entries are not allowed!";
                        
                        result.add(new org.xtreemfs.babudb.interfaces.LogEntry(
                                le.serialize(this.checksum)));
    
                        counter++;
                    } finally {
                        this.checksum.reset();
                        if (le != null) le.free();
                    }
                } while (le.getLSN().compareTo(end) < 0 && 
                        counter < MAX_LOGENTRIES_PER_REQUEST);
                
                // send the response, if the requested log entries are found
                Logging.logMessage(Logging.LEVEL_INFO, this, 
                        "REQUEST: returning %d log-entries to %s.", 
                        result.size(), rq.getRPCRequest().getClientIdentity());
                
                rq.sendSuccess(new replicaResponse(result));
            } catch (Exception e) {
                Logging.logError(Logging.LEVEL_INFO, this, e);
                
                // clear result
                for (org.xtreemfs.babudb.interfaces.LogEntry entry : result) {
                    BufferPool.free(entry.getPayload());
                }
                
                rq.sendReplicationException(ErrNo.BUSY,
                        "Request not finished: "+e.getMessage(), e);
            } finally {
                if (it != null) {
                    try {
                        it.destroy();
                    } catch (IOException e) { /* ignored */ }
                }
            }
        }
    }
}