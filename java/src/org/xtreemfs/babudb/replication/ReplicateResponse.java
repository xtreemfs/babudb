/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * Proxy for the overall response of broadcast requests.
 * 
 * @author flangner
 * @since 06/07/2009
 */

public final class ReplicateResponse extends LatestLSNUpdateListener {  

    private boolean        finished = false;
    private int            permittedFailures;
    private final LogEntry logEntry;
    
    /**
     * Initializes the response object waiting for the given {@link LSN} to become
     * the next stable state.
     * 
     * @param le - {@link LogEntry} associated with the {@link LSN}.
     * @param slavesThatCanFail - buffer for negative RPCResponses.
     */
    ReplicateResponse(LogEntry le, int slavesThatCanFail) {
        super(le.getLSN());
        logEntry = le;
        permittedFailures = slavesThatCanFail;
    }
    
    /**
     * Use this function to update the permitted failures on this response.
     */
    synchronized void decrementPermittedFailures(){
        if (permittedFailures == 0 && !finished) {
            finished = true;
            logEntry.getListener().failed(logEntry, new BabuDBException(
                    ErrorCode.REPLICATION_FAILURE,"LogEntry could not be " +
                    "replicated!"));
        }
        permittedFailures--;
    }
    
    /**
     * @return the original logEntry.
     */
    public LogEntry getLogEntry() {
        return this.logEntry;
    }
        
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LatestLSNUpdateListener#upToDate()
     */
    @Override
    public synchronized void upToDate() {
        if (!finished) {
            finished = true;
            logEntry.getListener().synced(logEntry);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LatestLSNUpdateListener#failed()
     */
    @Override
    public synchronized void failed() {
        if (!finished) {
            finished = true;
            logEntry.getListener().failed(logEntry, new Exception("Replication"+
                    " of LogEntry ("+logEntry.getLSN().toString()+") failed!"));
        }
    }
}