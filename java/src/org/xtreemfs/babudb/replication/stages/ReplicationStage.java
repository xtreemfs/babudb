/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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

import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.logic.LoadLogic;
import org.xtreemfs.babudb.replication.stages.logic.Logic;
import org.xtreemfs.babudb.replication.stages.logic.BasicLogic;
import org.xtreemfs.babudb.replication.stages.logic.LogicID;
import org.xtreemfs.babudb.replication.stages.logic.RequestLogic;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleThread;

import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;
import static org.xtreemfs.babudb.replication.operations.ErrNo.*;

/**
 * Stage to perform {@link ReplicationManager}-{@link Operation}s on the slave.
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class ReplicationStage extends LifeCycleThread {

    public final static int MAX_RETRIES = 3;
    public final static int RETRY_DELAY = 500;
        
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
    private int tries = 0;
    
    /**
     * 
     * @param dispatcher
     * @param max_q - 0 means infinite.
     * @param queue - the backup of an existing queue.
     * @param last - to load from.
     */
    public ReplicationStage(SlaveRequestDispatcher dispatcher, int max_q, BlockingQueue<StageRequest> queue, LSN last) {
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
        setLifeCycleListener(dispatcher);
        
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
            this.quit = true;
            this.interrupt();
        }
        missing = null;
        logicID = BASIC;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        notifyStarted();
        while (!quit) {
            try {
                if (tries != 0) Thread.sleep(RETRY_DELAY*tries);
                logics.get(logicID).run();
                tries = 0;
            } catch(ConnectionLostException cle) {
                
                switch (cle.errNo) {
                case LOG_CUT : 
                    setLogic(LOAD, "Master said, logfile was cut off.");
                    break;
                case TOO_BUSY :
                    if (++tries < MAX_RETRIES) break;
                case SERVICE_UNAVAILABLE : 
                    // fail-over: pauses()
                default :
                    Logging.logError(Logging.LEVEL_WARN, this, cle);
                    quit = true;
                    dispatcher.pauses(null);
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
        Logging.logMessage(Logging.LEVEL_INFO, this, "Replication logic changed: %s, because: %s", lgc.toString(), reason);
        this.logicID = lgc;
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
    public void clearQueue() {
        for (StageRequest rq : q) 
            finalizeRequest(rq);
    }

    /**
     * send an request for a stage operation
     *
     * @param rq
     *            the request
     * @param the
     *            method in the stage to execute
     *            
     * @throws TooBusyException
     */
    public void enqueueOperation(Object[] args) throws TooBusyException {
        if (numRequests.incrementAndGet()>MAX_Q && MAX_Q != 0){
            numRequests.decrementAndGet();
            throw new TooBusyException(getName()+": Operation could not be performed.");
        } else if (quit) 
            throw new TooBusyException(getName()+": Shutting down.");
        q.add(new StageRequest(args));
    }
    
    /**
     * <p>{@link Exception} to throw if there are 
     * to much requests proceeded at the moment.</p>
     * 
     * @author flangner
     * @since 06/08/2009
     */
    public static final class TooBusyException extends Exception {
        private static final long serialVersionUID = 2823332601654877350L; 
        public TooBusyException(String string) {
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