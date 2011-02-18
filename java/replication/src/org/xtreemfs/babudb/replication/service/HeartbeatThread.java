/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>Simple Thread for sending acknowledged {@link LSN}s to the master.</p>
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class HeartbeatThread extends LifeCycleThread implements Pacemaker {
    
    /** 10 seconds */
    public final static long            MAX_DELAY_BETWEEN_HEARTBEATS = 10*1000; 
    
    /** approach to get the master to send the heartbeat messages to */
    private final ParticipantsOverview  pOverview;
    
    /** holds the identifier of the last written LogEntry. */
    private volatile LSN                latestLSN;
       
    /** set to true, if thread should shut down */
    private volatile boolean            quit = false;
    
    /** set to true, if the thread should be stopped temporarily */
    private final AtomicBoolean         halted = new AtomicBoolean(false);
    
    /**
     * Default constructor with the initial values.
     * 
     * @param pOverview
     */
    public HeartbeatThread(ParticipantsOverview pOverview) {
        super("HeartbeatThread");
        this.pOverview = pOverview;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.Pacemaker#updateLSN(
     *          org.xtreemfs.babudb.lsmdb.LSN)
     */
    public synchronized void updateLSN(LSN lsn) {
        if (latestLSN.compareTo(lsn) < 0) {
            latestLSN = lsn;
            synchronized (halted) {
                if (halted.compareAndSet(true, false)) halted.notify();
                else interrupt();
            }
        }
    }

    /**
     * Starts the heartbeat initialized with the given {@link LSN}.
     * @param initial
     */
    public synchronized void start(LSN initial) {
        this.latestLSN = initial;
        super.start();
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        this.quit = false;
        notifyStarted();
        
        while (!this.quit) {
            processHeartbeat();
            
            try {
                sleep(MAX_DELAY_BETWEEN_HEARTBEATS);
            } catch(InterruptedException e) { 
                /* ignored */
            }
            
            synchronized (halted) {
                try {
                    if (halted.get())
                        halted.wait();
                } catch (InterruptedException e) {
                    if (!quit) notifyCrashed(e);
                    assert (quit==true) : "Halted must be notified, if thread "+
                    		"should not shut down";
                }
            }
        }
        
        notifyStopped();
    }

    /**
     * Sends a heartBeat message to the master.
     */
    @SuppressWarnings({ "unchecked"})
    private void processHeartbeat() {
        final ConditionClient c = this.pOverview.getMaster();
        if (c != null) {
            ((ClientResponseFuture<Object>) c.heartbeat(latestLSN))
                .registerListener(new ClientResponseAvailableListener<Object>() {
              
                @Override
                public void responseAvailable(Object r) { 
                    Logging.logMessage(Logging.LEVEL_NOTICE, this, 
                            "Heartbeat successfully send.");
                }

                @Override
                public void requestFailed(Exception e) {
                    Logging.logMessage(Logging.LEVEL_WARN, this, 
                            "Heartbeat could not be send to %s, because %s", 
                            c.toString(), e.getMessage());
                    Logging.logError(Logging.LEVEL_DEBUG, this, e);
                }
            });
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.Pacemaker#infarction()
     */
    public void infarction() {
        synchronized (halted) {
            if (halted.compareAndSet(false, true)) interrupt();
        }
    }
    
    /**
     * shut the thread down
     */
    public void shutdown() {
        if (quit != true) {
            this.quit = true;
            this.interrupt();
        }
    }
}