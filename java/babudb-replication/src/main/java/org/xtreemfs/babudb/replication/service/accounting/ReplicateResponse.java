/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.accounting;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * Proxy for the overall response of broadcast requests.
 * 
 * @author flangner
 * @since 06/07/2009
 */

public final class ReplicateResponse extends LatestLSNUpdateListener {  

    private boolean             finished = false;
    private int                 permittedFailures;
    private final SyncListener  listener;
    
    /**
     * Dummy constructor to respond already failed requests.
     * 
     * @param le
     * @param error
     */
    public ReplicateResponse(LogEntry le, Exception error) {
        super(le.getLSN());
        
        this.finished = true;
        this.permittedFailures = -1;
        this.listener = le.getListener();
        this.listener.failed(error);
    }
    
    /**
     * Dummy constructor to respond already failed requests.
     * 
     * @param lsn
     * @param listener
     * @param error
     */
    public ReplicateResponse(LSN lsn, SyncListener listener, Exception error) {
        super(lsn);
        
        this.finished = true;
        this.permittedFailures = -1;
        this.listener = listener;
        this.listener.failed(error);
    }
    
    /**
     * Initializes the response object waiting for the given {@link LSN} to become
     * the next stable state.
     * 
     * @param le - {@link LogEntry} associated with the {@link LSN}.
     * @param slavesThatCanFail - buffer for negative RPCResponses.
     */
    public ReplicateResponse(LogEntry le, int slavesThatCanFail) {
        super(le.getLSN());
        this.listener = le.getListener();
        this.permittedFailures = slavesThatCanFail;
    }
    
    /**
     * Initializes the response object waiting for the given {@link LSN} to become
     * the next stable state.
     * 
     * @param lsn
     * @param listener
     * @param slavesThatCanFail - buffer for negative RPCResponses.
     */
    public ReplicateResponse(LSN lsn, SyncListener listener, int slavesThatCanFail) {
        super(lsn);
        this.listener = listener;
        this.permittedFailures = slavesThatCanFail;
    }
    
    /**
     * Use this function to update the permitted failures on this response.
     */
    public synchronized void decrementPermittedFailures(){
        if (permittedFailures == 0 && !finished) {
            finished = true;
            listener.failed(new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                    "LogEntry could not be replicated!"));
        }
        permittedFailures--;
    }
    
    /**
     * @return true if this response indicates, that its request has already 
     *         failed. false otherwise.
     */
    public synchronized boolean hasFailed() {
        return finished && permittedFailures < 0;
    }
        
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LatestLSNUpdateListener#upToDate()
     */
    @Override
    public synchronized void upToDate() {
        if (!finished) {
            finished = true;
            listener.synced(lsn);
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
            listener.failed(new Exception("Replication of LogEntry (" + lsn.toString() 
                    + ") failed!"));
        }
    }
}