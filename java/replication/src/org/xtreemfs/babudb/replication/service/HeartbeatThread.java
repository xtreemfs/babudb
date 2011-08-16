/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
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
    
    public final static long            MAX_DELAY_BETWEEN_HEARTBEATS = 
        ReplicationConfig.MESSAGE_TIMEOUT; 
    
    /** approach to get the master to send the heartbeat messages to */
    private final ParticipantsOverview  pOverview;
    
    /** holds the identifier of the last written LogEntry. */
    private LSN                         latestLSN;
    
    /** set to true, if the thread should be stopped temporarily */
    private boolean                     hasInfarct = false;
       
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
    public HeartbeatThread(ParticipantsOverview pOverview, int localPort) {
        super("HeartbeatThread");
        this.pOverview = pOverview;
        this.localPort = localPort;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.Pacemaker#updateLSN(
     *          org.xtreemfs.babudb.lsmdb.LSN)
     */
    public synchronized void updateLSN(LSN lsn) {
        if (latestLSN.compareTo(lsn) < 0) {
            latestLSN = lsn;
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
                processHeartbeat();
                
                synchronized (this) {
                    wait(MAX_DELAY_BETWEEN_HEARTBEATS);
                    
                    while (hasInfarct) {
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
    private void processHeartbeat() {
        for (final ConditionClient c : pOverview.getConditionClients()) {
            c.heartbeat(latestLSN, localPort).registerListener(
                    new ClientResponseAvailableListener<Object>() {
              
                @Override
                public void responseAvailable(Object r) { 
                    
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                            "Heartbeat successfully send.");
                }

                @Override
                public void requestFailed(Exception e) {
                   
                    Logging.logMessage(Logging.LEVEL_INFO, this, 
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
        hasInfarct = true;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.Pacemaker#hasInfarct()
     */
    public synchronized boolean hasInfarct() {
        return hasInfarct;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.Pacemaker#reanimate()
     */
    @Override
    public synchronized void reanimate() {
        if (hasInfarct) {
            hasInfarct = false;
            notify();
        }
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