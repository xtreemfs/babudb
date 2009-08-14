/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.events;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.LatestLSNUpdateListener;

/**
 * Proxy for the overall response of broadcast requests.
 * 
 * @author flangner
 * @since 06/07/2009
 */

public class EventResponse extends LatestLSNUpdateListener {  

    private boolean failed = false;
    private boolean finished = false;
    private int     permittedFailures;
    
    /**
     * Initializes the response object waiting for the given {@link LSN} to become
     * the next stable state.
     * 
     * @param lsn
     * @param slavesThatCanFail - buffer for negative RPCResponses.
     */
    public EventResponse(LSN lsn, int slavesThatCanFail) {
        super(lsn);
        permittedFailures = slavesThatCanFail;
    }
    
    /**
     * Use this function to update the permitted failures on this response.
     */
    public synchronized void decrementPermittedFailures(){
        if (--permittedFailures == 0 && !finished) {
            failed = true;
            finished = true;
            notify();
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LatestLSNUpdateListener#upToDate()
     */
    @Override
    public synchronized void upToDate() {
        finished = true;
        notify();
    }
    
    /**
     * Waits synchronously for the result.
     * 
     * @return true if the broadcast succeeded, false otherwise.
     * @throws InterruptedException 
     */
    public synchronized boolean succeeded() throws InterruptedException {
        if (!finished) wait();
        return !failed;
    }
}