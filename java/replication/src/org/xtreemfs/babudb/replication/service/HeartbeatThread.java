/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.util.List;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
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
    
    /** just a second in ms */
    public final static long            MAX_DELAY_BETWEEN_HEARTBEATS = 1000; 
    
    /** approach to get the master to send the heartbeat messages to */
    private final ParticipantsStates    pOverview;
    
    /** holds the identifier of the last written LogEntry. */
    private LSN                         latestLSN;
    
    /** set to true, if the thread should be stopped temporarily */
    private boolean                     halted = false;
       
    /** set to true, if thread should shut down */
    private volatile boolean            quit = false;
    
    /** port that identifies the local service */
    private final int                   localPort;
    
    /**
     * Default constructor with the initial values.
     * 
     * @param pOverview
     * @param localPort
     */
    public HeartbeatThread(ParticipantsStates pOverview, int localPort) {
        super("HeartbeatThread");
        this.pOverview = pOverview;
        this.localPort = localPort;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.Pacemaker#updateLSN(
     *          org.xtreemfs.babudb.lsmdb.LSN)
     */
    public synchronized void updateLSN(LSN lsn) {
        if (latestLSN.compareTo(lsn) < 0 || halted) {
            latestLSN = lsn;
            halted = false;
            notify();
        }
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public void start() {
        throw new UnsupportedOperationException("Use start(LSN initial) instead!");
    }

    /**
     * Starts the heartbeat initialized with the given {@link LSN}.
     * @param initial
     */
    public void start(LSN initial) {
        latestLSN = initial;
        processHeartbeat(pOverview.getConditionClients());
        super.start();
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        quit = false;
        notifyStarted();
        
        try {
            while (!quit) {
                processHeartbeat(pOverview.getSafeConditionClients());
                
                synchronized (this) {
                    wait(MAX_DELAY_BETWEEN_HEARTBEATS);
                    
                    if (halted) {
                        wait();
                    }
                }
            }
        } catch (InterruptedException e) {
            if (!quit) notifyCrashed(e);
        }
        
        notifyStopped();
    }

    /**
     * Sends a heartbeat message to all available servers.
     */
    private void processHeartbeat(List<ConditionClient> clients) {
        for (final ConditionClient c : clients) {
            c.heartbeat(latestLSN, localPort).registerListener(
                    new ClientResponseAvailableListener<Object>() {
              
                @Override
                public void responseAvailable(Object r) { 
                    
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                            "Heartbeat successfully send.");
                }

                @Override
                public void requestFailed(Exception e) {
                    
                    pOverview.markAsDead(c);
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
    public synchronized void infarction() {
        halted = true;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleThread#shutdown()
     */
    @Override
    public void shutdown() {
        quit = true;
        interrupt();
    }
}