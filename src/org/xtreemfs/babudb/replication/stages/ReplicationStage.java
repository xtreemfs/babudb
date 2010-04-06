/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationManagerImpl;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.logic.LoadLogic;
import org.xtreemfs.babudb.replication.stages.logic.Logic;
import org.xtreemfs.babudb.replication.stages.logic.BasicLogic;
import org.xtreemfs.babudb.replication.stages.logic.LogicID;
import org.xtreemfs.babudb.replication.stages.logic.RequestLogic;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.replication.operations.ErrNo.*;
import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;

/**
 * Stage to perform {@link ReplicationManagerImpl}-{@link Operation}s on the slave.
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class ReplicationStage extends LifeCycleThread {

    private final static int                    RETRY_DELAY_MS = 500;
        
    public LSN                                  lastInserted;
    
    public final SlaveRequestDispatcher         dispatcher;
    
    /** missingRange shared between PLAIN and REQUEST {@link Logic} */
    public LSNRange missing = null;
    
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
    
    private BabuDBRequest<Boolean>              listener = null;
    
    /**
     * 
     * @param dispatcher
     * @param max_q - 0 means infinite.
     * @param queue - the backup of an existing queue.
     * @param last - to load from.
     */
    public ReplicationStage(SlaveRequestDispatcher dispatcher, int max_q, 
            BlockingQueue<StageRequest> queue, LSN last) {
        
        super("ReplicationStage");
        
        if (queue!=null) {
            this.q = queue;
            this.numRequests = new AtomicInteger(queue.size());
        } else {
            this.q = new PriorityBlockingQueue<StageRequest>();
            this.numRequests = new AtomicInteger(0);
        }
        this.MAX_Q = max_q;
        this.quit = false;
        this.dispatcher = dispatcher;
        this.lastInserted = last;
        
        // ---------------------
        // initialize logic
        // ---------------------
        logics = new HashMap<LogicID, Logic>();
        
        Logic lg = new BasicLogic(this,q);
        logics.put(lg.getId(), lg);
        
        lg = new RequestLogic(this);
        logics.put(lg.getId(), lg);
        
        lg = new LoadLogic(this);
        logics.put(lg.getId(), lg);
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
                if (tries != 0) Thread.sleep(RETRY_DELAY_MS*tries);
                
                // prevent the heartbeatThread from sending messages as long as
                // this server is synchronizing with another server
                if (!infarcted && !logicID.equals(BASIC)) {
                    dispatcher.heartbeat.infarction();
                    infarcted = true;
                }
                
                // operate the request
                logics.get(logicID).run();
                
                // update the heartbeatThread and re-animate it
                if (infarcted && logicID.equals(BASIC)) { 
                    dispatcher.updateLatestLSN(lastInserted);
                    infarcted = false;
                }
                
                // reset the number of tries
                tries = 0;
            } catch(ConnectionLostException cle) {
                
                switch (cle.errNo) {
                case LOG_REMOVED : 
                    setLogic(LOAD, "Master said, logfile was cut off.");
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
                    dispatcher.suspend();
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
    public void manualLoad(BabuDBRequest<Boolean> listener, LSN from, LSN to) {
        this.listener = listener;
        
        org.xtreemfs.babudb.interfaces.LSN start = 
            new org.xtreemfs.babudb.interfaces.LSN(
                    from.getViewId(), from.getSequenceNo());
        org.xtreemfs.babudb.interfaces.LSN end = 
            new org.xtreemfs.babudb.interfaces.LSN(
                    to.getViewId(), to.getSequenceNo());
        missing = new LSNRange(start, end);
        setLogic(REQUEST, "manually synchronization");
        
        // necessary to wake up the mechanism
        if (q.isEmpty()) q.add(new StageRequest(null));
    }
    
    /**
     * <p>Frees the given operation and decrements the number of requests.</p>
     * 
     * @param op
     */
    public void finalizeRequest(StageRequest op) {
        if (op.getArgs()[1] != null && op.getArgs()[1] instanceof LogEntry)
            ((LogEntry) op.getArgs()[1]).free();
        
        op.free();
        int numRqs = numRequests.decrementAndGet();
        assert(numRqs >= 0) : "The number of requests cannot be negative, especially not '"+numRqs+"'.";
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

    /**
     * send an request for a stage operation
     *
     * @param rq
     *            the request
     * @param the
     *            method in the stage to execute
     *            
     * @throws BusyServerException
     */
    public void enqueueOperation(Object[] args) throws BusyServerException {
        if (numRequests.incrementAndGet()>MAX_Q && MAX_Q != 0){
            numRequests.decrementAndGet();
            throw new BusyServerException(getName()+": Operation could not be performed.");
        } else if (quit) 
            throw new BusyServerException(getName()+": Shutting down.");
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
        
        public ConnectionLostException(String string, int errorNumber) {
            super("Connection to the participant is lost: "+string);
            this.errNo = errorNumber;
        }
    }
}