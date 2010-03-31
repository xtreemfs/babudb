/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;

/**
 * Component that checks regularly the time-drift of all available replication
 * participants.
 * 
 * @author flangner
 * @since 03/30/2010
 */
class TimeDriftDetector {
    
    /** The Listener to inform about an illegal time-drift. */
    private final TimeDriftListener     listener;
    
    /** The clients to check the time at. */
    private final StateClient[]         participants;
    
    /** The {@link Timer} to schedule the detection-task at. */
    private final Timer                 timer;
    
    /** */
    private final int                   maxDrift;
    
    /** The minimum delay between two checks. */
    private final long                  DELAY_BETWEEN_CHECKS;
    
    /** The check task to execute after delay was running out. */
    private TimerTask                   task = null;
    
    /**
     * Initializes the detector. 
     * 
     * @param listener - to be informed, if an illegal time-drift was detected.
     * @param participants - to check for illegal drifts at.
     * @param client - to use to connect to the participants.
     * @param dMax - maximum time-drift allowed.
     */
    TimeDriftDetector(TimeDriftListener listener, 
            Set<InetSocketAddress> participants, RPCNIOSocketClient client, 
            int dMax) {
        this.listener = listener;
        this.participants = new StateClient[participants.size()];
        
        int pos = 0;
        for (InetSocketAddress address : participants)
            this.participants[pos++] = new StateClient(client, address, null);
            
        this.timer = new Timer("TimeDriftDetector", true);
        
        this.DELAY_BETWEEN_CHECKS = participants.size() * 
                                    ReplicationConfig.REQUEST_TIMEOUT;
        
        this.maxDrift = dMax;
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
    }
    
    /**
     * Removes the detection task.
     */
    void stop() {
        synchronized (this) {
            this.task.cancel();
            this.task = null;
        }
    }

    /**
     * Terminates the timer. This could not be started again.
     */
    void shutdown() {
        this.timer.cancel();
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
            for (StateClient client : participants) {
                RPCResponse<Long> rp = null;
                try {
                    start = TimeSync.getGlobalTime();
                    rp = client.getLocalTime();
                    cTime = rp.get();
                    end = TimeSync.getGlobalTime();
                    
                    if (cTime < (start - maxDrift) || cTime > (end + maxDrift)) {
                        listener.driftDetected();
                        break;
                    }
                } catch (Throwable e) {
                    Logging.logMessage(Logging.LEVEL_INFO, timer, 
                            "Local time of '%s' could not be fetched.", 
                            client.getDefaultServerAddress().toString());
                } finally {
                    if (rp != null) rp.freeBuffers();
                }
            }
        }
    }
}
