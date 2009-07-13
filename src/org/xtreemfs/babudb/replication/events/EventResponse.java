/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.events;

import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.LatestLSNUpdateListener;

/**
 * Proxy for the overall response of broadcast requests.
 * 
 * @author flangner
 * @since 06/07/2009
 */

public class EventResponse extends LatestLSNUpdateListener {  

    private final AtomicInteger permittedFailures;
    
    private boolean failed = false;
    private boolean finished = false;
    
    /**
     * Initializes the response object waiting for the given {@link LSN} to become
     * the next stable state.
     * 
     * @param lsn
     * @param slavesThatCanFail - buffer for negative RPCResponses.
     */
    public EventResponse(LSN lsn, int slavesThatCanFail) {
        super(lsn);
        this.permittedFailures = new AtomicInteger(slavesThatCanFail);
    }
    
    /**
     * Use this function to update the permitted failures on this response.
     */
    public void decrementPermittedFailures(){
        if (permittedFailures.getAndDecrement() == 0) {
            synchronized (this) {
                assert(!finished);
                failed = true;
                finished = true;
                this.notify();
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LatestLSNUpdateListener#upToDate()
     */
    @Override
    public void upToDate() {
        synchronized (this) {
            finished = true;
            this.notify();
        }
    }
    
    /**
     * Waits synchronously for the result.
     * 
     * @return true if the broadcast succeeded, false otherwise.
     * @throws InterruptedException 
     */
    public boolean succeeded() throws InterruptedException {
        synchronized (this) {
            if (!finished) this.wait();
        }
        return !failed;
    }
}