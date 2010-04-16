/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;

/**
 * Component that checks regularly the time-drift of all available replication
 * participants.
 * 
 * @author flangner
 * @since 03/30/2010
 */
 class TimeDriftDetector {
    
    /** listener to inform about an illegal time-drift */
    private final TimeDriftListener     listener;
    
    /** clients to check the time at */
    private final List<ConditionClient> participants;
    
    /** {@link Timer} to schedule the detection-task at */
    private final Timer                 timer;
    
    /** */
    private final int                   maxDrift;
    
    /** minimum delay between two checks */
    private final long                  DELAY_BETWEEN_CHECKS;
    
    /** check task to execute after delay was running out */
    private TimerTask                   task = null;
    
    /** listener to inform about lifeCycle events */
    private volatile LifeCycleListener  lifeCyclelistener = null;
    
    /**
     * Initializes the detector. 
     * 
     * @param listener - to be informed, if an illegal time-drift was detected.
     * @param clients - to connect with the servers where to check for illegal 
     *                  drifts at.
     * @param client - to use to connect to the participants.
     * @param dMax - maximum time-drift allowed.
     */
    TimeDriftDetector(TimeDriftListener listener, List<ConditionClient> clients, 
            int dMax) {
        this.listener = listener;
        this.participants = clients;
                    
        this.timer = new Timer("TimeDriftDetector", true);
        
        this.DELAY_BETWEEN_CHECKS = participants.size() * 
                                    ReplicationConfig.REQUEST_TIMEOUT;
        
        this.maxDrift = dMax;
    }
    
    void setLifeCycleListener(LifeCycleListener listener) {
        assert (listener != null);
        this.lifeCyclelistener = listener;
    }
    
    /**
     * Schedules the detection task.
     * 
     * @throws IllegalStateException if this has been shut down jet.
     */
    void start() throws IllegalStateException{
        synchronized (this) {
            if (this.task == null) { 
                this.task = new CheckTask();
                this.timer.schedule(this.task, DELAY_BETWEEN_CHECKS);
            }
        }
        if (this.lifeCyclelistener != null) 
            this.lifeCyclelistener.startupPerformed(); 
    }
    
    /**
     * Removes the detection task.
     */
    void stop() {
        synchronized (this) {
            if (this.task != null) {
                this.task.cancel();
                this.task = null;
                this.timer.purge();
            }
        }
    }

    /**
     * Terminates the timer. This could not be started again.
     */
    void shutdown() {
        this.timer.cancel();
        if (this.lifeCyclelistener != null) 
            this.lifeCyclelistener.shutdownPerformed();
    }
    
    
    
    /**
     * Listener to be informed about the detection of an illegal time-drift.
     * 
     * @author flangner
     * @since 03/30/2010
     */
    interface TimeDriftListener {
        
        /**
         * This method is executed if an illegal time-drift was detected.
         */
        void driftDetected();
    }
    
    /**
     * Class for the check task performed by the TimeDriftDetector.
     * 
     * @author flangner
     * @since 03/30/2010
     */
    private final class CheckTask extends TimerTask {
        
        /*
         * (non-Javadoc)
         * @see java.util.TimerTask#run()
         */
        @Override
        public void run() {
            
            long start;
            long end;
            long cTime;
            for (ConditionClient client : participants) {
                RPCResponse<Long> rp = null;
                try {
                    start = TimeSync.getGlobalTime();
                    rp = client.time();
                    cTime = rp.get();
                    end = TimeSync.getGlobalTime();
                    
                    if (cTime < (start - maxDrift) || 
                        cTime > (end + maxDrift)) {
                        
                        listener.driftDetected();
                        break;
                    }
                } catch (Throwable e) {
                    Logging.logMessage(Logging.LEVEL_INFO, timer, 
                            "Local time of '%s' could not be fetched.", 
                            client.toString());
                } finally {
                    if (rp != null) rp.freeBuffers();
                }
            }
        }
    }
}
