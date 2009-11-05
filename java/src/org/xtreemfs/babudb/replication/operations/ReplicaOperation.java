/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaResponse;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.log.DiskLogFile;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.MasterRequestDispatcher;
import org.xtreemfs.babudb.replication.Request;
import org.xtreemfs.include.common.logging.Logging;

/**
 * {@link Operation} to request {@link LogEntry}s from the master.
 * 
 * @since 05/03/2009
 * @author flangner
 */

public class ReplicaOperation extends Operation {

    private final int procId;
    
    private final MasterRequestDispatcher dispatcher;
    
    private final Checksum checksum = new CRC32();
    
    public ReplicaOperation(MasterRequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        procId = new replicaRequest().getTag();
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
    public Serializable parseRPCMessage(Request rq) {
        replicaRequest rpcrq = new replicaRequest();
        rq.deserializeMessage(rpcrq);  
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
        replicaRequest req = (replicaRequest) rq.getRequestMessage();
        LSN start = new LSN(req.getRange().getViewId(),req.getRange().getSequenceStart());
        int numOfLEs = (int) (req.getRange().getSequenceEnd()-req.getRange().getSequenceStart());
        LogEntries result = new LogEntries();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST received " +
        	"(start: %s, numOfLEs: %d)", start.toString(), numOfLEs);
        
        int i = 0;
        LogEntry le = null;
        // get the latest logFile
        DiskLogFile dlf = null;
        try {
            dlf = new DiskLogFile(dispatcher.dbs.getLogger().getLatestLogFileName());
            
            // get the first logEntry
            while (dlf.hasNext() && i == 0) {
                try {
                    le = dlf.next();
                    if (le.getLSN().equals(start)) {
                        assert (le.getPayload().array().length > 0) : "Empty logentries are not allowed anymore!";
                        result.add(new org.xtreemfs.babudb.interfaces.LogEntry(le.serialize(checksum)));
                        i++;
                    }
                } catch (LogEntryException e) {
                    rq.sendReplicationException(ErrNo.LOG_CUT,"LogEntry unavailable: "+e.getMessage());
                    i = -1;
                } finally {
                    checksum.reset();
                  // clears the buffer, before it gets the next entry
                    if (le!=null) le.free();
                }
            }
                
            
            // get the remaining requested logEntries          
            if (i > 0) {
                try {
                    for (int j=i;j<numOfLEs;j++) {
                        le = dlf.next();           
                        assert (le.getPayload().array().length > 0) : "Empty logentries are not allowed anymore!";
                        result.add(new org.xtreemfs.babudb.interfaces.LogEntry(le.serialize(checksum)));
                        checksum.reset();
                        le.free();
                    }
                } catch (LogEntryException e) {
                    rq.sendReplicationException(ErrNo.LOG_CUT,"LogEntry unavailable: "+e.getMessage());
                    i = -1;
                } finally {
                    checksum.reset();
                  // clears the buffer, before it gets the next entry
                    if (le!=null) le.free();
                }
            }
            
            // send the response, if the requested log entries are found
            if (i > 0) rq.sendSuccess(new replicaResponse(result));
            // send a replication exception if not done so far
            else if (i==0) rq.sendReplicationException(ErrNo.LOG_CUT,"LogEntry unavailable.");
        } catch (IOException e) {
            rq.sendReplicationException(ErrNo.INTERNAL_ERROR,"Request not finished: "+e.getMessage());
        } finally {
            if (dlf!=null) {
                try {
                    dlf.close();
                } catch (IOException e) { /* ignored */ }
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.operations.Operation#canBeDisabled()
     */
    @Override
    public boolean canBeDisabled() {
        return true;
    }
}