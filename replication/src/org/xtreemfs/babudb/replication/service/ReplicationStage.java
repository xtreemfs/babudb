/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.control.RoleChangeListener;
import org.xtreemfs.babudb.replication.service.logic.BasicLogic;
import org.xtreemfs.babudb.replication.service.logic.LoadLogic;
import org.xtreemfs.babudb.replication.service.logic.Logic;
import org.xtreemfs.babudb.replication.service.logic.LogicID;
import org.xtreemfs.babudb.replication.service.logic.RequestLogic;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.replication.service.logic.LogicID.*;
import static org.xtreemfs.babudb.replication.transmission.ErrorCode.*;

/**
 * Stage to perform {@link ReplicationManager}-{@link Operation}s on the slave.
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class ReplicationStage extends LifeCycleThread implements RequestManagement {

    private final static int                    RETRY_DELAY_MS = 500;
        
    public  LSN                                 lastInserted;
    
    public final AtomicReference<LSN>           lastOnView;
        
    /** 
     * {@link LSN} range of missing {@link LogEntries} shared between the 
     * {@link Logic} and requested at the other server.
     * Start of the range will always be the LSN of the last inserted 
     * {@link LogEntry}. End the last entry that was requested to be replicated.
     */
    public Range                                missing = null;
    
    /** {@link LogicID} of the {@link Logic} currently used. */
    private LogicID                             logicID = BASIC;
    
    /** queue containing all incoming requests */
    private final BlockingQueue<StageRequest>   q;
        
    /** set to true if stage should shut down */
    private volatile boolean                    quit;
    
    /** used for load balancing */
    private final int                           MAX_Q;
    private final AtomicInteger                 numRequests;
    
    /** Table of different replication-behavior objects. */
    private final Map<LogicID, Logic>           logics;
    
    /** Counter for the number of tries needed to perform an operation */
    private int                                 tries = 0;
    
    private BabuDBRequestResultImpl<Boolean>    listener = null;
    
    /** needed to control the heartbeat */
    protected final Pacemaker                   pacemaker;

    /** listener to notify about the need of a role change */
    private RoleChangeListener                  roleChangeListener = null;
    
    /**
     * @param lastOnView
     * @param slaveView
     * @param roleChangeListener - listener to notify about the need of a role 
     *                             change.
     * @param pacemaker - for the hearbeat.
     * @param max_q - 0 means infinite.
     * @param lastOnView - reference for the compare-variable lastOnView.
     */
    public ReplicationStage(int max_q, Pacemaker pacemaker, SlaveView slaveView, 
            FileIOInterface fileIO, BabuDBInterface babuInterface, 
            AtomicReference<LSN> lastOnView, int maxChunkSize) {
        
        super("ReplicationStage");

        this.lastOnView = lastOnView;
        this.pacemaker = pacemaker;
        this.q = new PriorityBlockingQueue<StageRequest>();
        this.numRequests = new AtomicInteger(0);
        this.MAX_Q = max_q;
        this.quit = false;
        
        // ---------------------
        // initialize logic
        // ---------------------
        logics = new HashMap<LogicID, Logic>();
        
        
        Logic lg = new BasicLogic(this, pacemaker, slaveView, fileIO, 
                babuInterface, q);
        logics.put(lg.getId(), lg);
        
        lg = new RequestLogic(this, pacemaker, slaveView, fileIO, 
                babuInterface);
        logics.put(lg.getId(), lg);
        
        lg = new LoadLogic(this, pacemaker, slaveView, fileIO, babuInterface, 
                           maxChunkSize);
        logics.put(lg.getId(), lg);
    }

    /**
     * Register the {@link RoleChangeListener}.
     * 
     * @param roleChangeListener
     */
    public void setRoleChangeListener(RoleChangeListener roleChangeListener) {
        this.roleChangeListener = roleChangeListener;
    }
    
    /**
     * Shut the stage thread down.
     * Resets inner state.
     */
    public void shutdown() {
        if (quit!=true) {
            quit = true;
            interrupt();
        }
        missing = null;
        logicID = BASIC;
        clearQueue();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        notifyStarted();
        while (!quit) {
            boolean infarcted = false;
            try {
                // sleep if a request shall be retried 
                if (tries != 0) Thread.sleep(RETRY_DELAY_MS * tries);
                
                // prevent the heartbeatThread from sending messages as long as
                // this server is synchronizing with another server
                if (!infarcted && !logicID.equals(BASIC)) {
                    
                    this.pacemaker.infarction();
                    infarcted = true;
                }
                
                // operate the request
                logics.get(logicID).run();
                
                // update the heartbeatThread and re-animate it
                if (infarcted && logicID.equals(BASIC)) { 
                    this.pacemaker.updateLSN(lastInserted);
                    infarcted = false;
                }
                
                // reset the number of tries
                tries = 0;
            } catch(ConnectionLostException cle) {
                
                switch (cle.errNo) {
                case LOG_UNAVAILABLE : 
                    setLogic(LOAD, "Master said, logfile was cut off: " +
                            cle.getMessage());
                    break;
                case BUSY :
                    if (++tries < ReplicationConfig.MAX_RETRIES) break;
                case SERVICE_UNAVAILABLE : 
                    // fail-over: suspend()
                default :
                    Logging.logError(Logging.LEVEL_WARN, this, cle);
                    quit = true;
                    if (listener != null) {
                        listener.failed(new BabuDBException(
                                ErrorCode.REPLICATION_FAILURE,
                                cle.getMessage()));
                    }
                    this.roleChangeListener.suspend();
                    break;
                }
            } catch(InterruptedException ie) {
                if (!quit){
                    quit = true;
                    notifyCrashed(ie);
                    return;
                }
            }
        }
        
        notifyStopped();
    }
    
    /**
     * <p>Changes the currently used logic to the given <code>lgc</code>.</p>
     * <b>WARNING: This operation is not thread safe!</b>
     * <br>
     * @param lgc
     * @param reason - for the logic change, needed for logging purpose.
     */
    public void setLogic(LogicID lgc, String reason) {
        setLogic(lgc, reason, false);
    }
    
    /**
     * <p>Changes the currently used logic to the given <code>lgc</code>.</p>
     * <b>WARNING: This operation is not thread safe!</b>
     * <br>
     * @param lgc
     * @param reason - for the logic change, needed for logging purpose.
     * @param loaded - true if the database was loaded remotely, false otherwise.
     */
    public void setLogic(LogicID lgc, String reason, boolean loaded) {
        Logging.logMessage(Logging.LEVEL_INFO, this, 
                "Replication logic changed: %s, because: %s", 
                lgc.toString(), reason);
        
        this.logicID = lgc;
        if (listener != null && lgc.equals(BASIC)){
            listener.finished(!loaded);
            listener = null;
        }
    }
    
    /**
     * Method to trigger a {@link LOAD} manually.
     * Listener will contain true on finish , if it was a request synchronization,
     * or if just the log file was switched. 
     * otherwise, if it was a load, the listener will notified with false.
     * 
     * @param listener
     * 
     * @return 
     */
    public void manualLoad(BabuDBRequestResultImpl<Boolean> listener, LSN from, 
            LSN to) {
        assert (from.compareTo(to) < 0);
        
        this.listener = listener;
        
        LSN end = new LSN(
                    // we have to assume, that there is an entry following the
                    // state we would like to establish, because the request
                    // logic will always exclude the 'end' of this range
                    to.getViewId(), to.getSequenceNo() + 1L);
        
        missing = new Range(from, end);
        setLogic(REQUEST, "manually synchronization");
        
        // necessary to wake up the mechanism
        if (q.isEmpty()) q.add(new StageRequest(null));
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.RequestManagement#finalizeRequest(
     *          org.xtreemfs.babudb.replication.service.StageRequest)
     */
    public void finalizeRequest(StageRequest op) {        
        op.free();
        
        int numRqs = numRequests.decrementAndGet();
        assert(numRqs >= 0) : "The number of requests cannot be negative, especially not '" + 
                                numRqs + "'.";
    }

    /**
     * Needed for resetting the dispatcher.
     * 
     * @return the queue of the replication stage.
     */
    public BlockingQueue<StageRequest> backupQueue() {
        return q;
    }
    
    /**
     * Takes the requests from the queue and frees there buffers.
     */
    private void clearQueue() {
        StageRequest rq = q.poll();
        while (rq != null) {
            finalizeRequest(rq);
            rq = q.poll();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.RequestManagement#
     * enqueueOperation(java.lang.Object[])
     */
    public void enqueueOperation(Object[] args) throws BusyServerException {
        if (numRequests.incrementAndGet() > MAX_Q && MAX_Q != 0){
            numRequests.decrementAndGet();
            throw new BusyServerException(getName() + ": Operation could not " +
            		"be performed.");
        } else if (quit) {
            throw new BusyServerException(getName() + ": Shutting down.");
        }
        q.add(new StageRequest(args));
    }
    
    /**
     * <p>{@link Exception} to throw if there are 
     * to much requests proceeded at the moment.</p>
     * 
     * @author flangner
     * @since 06/08/2009
     */
    public static final class BusyServerException extends Exception {
        private static final long serialVersionUID = 2823332601654877350L; 
        public BusyServerException(String string) {
            super("Participant is too busy at the moment: "+string);
        }
    }
    
    /**
     * <p>{@link Exception} to throw if the 
     * connection to the master is lost.</p>
     * 
     * @author flangner
     * @since 08/14/2009
     */
    public static final class ConnectionLostException extends Exception {
        private static final long serialVersionUID = -167881170791343478L;
        int errNo;
        
        public ConnectionLostException(int errorNumber) {
            this.errNo = errorNumber;
        }
        
        public ConnectionLostException(String string, int errorNumber) {
            super("Connection to the participant is lost: " + string);
            this.errNo = errorNumber;
        }
    }
    
    /**
     * @author flangner
     * @since 01/05/2011
     */
    public static final class Range {
        public final LSN start;
        public final LSN end;
        
        public Range(LSN start, LSN end) {
            this.start = start;
            this.end = end;
        }
    }
}