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
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBReplication;
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

/**
 * Stage to perform {@link BabuDBReplication}-{@link Operation}s on the slave.
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class ReplicationStage extends LifeCycleThread {

    /** stage methods */
    public final static int CREATE      = 1;
    public final static int COPY        = 2;
    public final static int DELETE      = 3;
    public final static int REPLICATE   = 4;
    
    public volatile LSN                     loadUntil = new LSN(1,0L);
    
    public final SlaveRequestDispatcher     dispatcher;
    
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
    private final AtomicInteger                 numRequests = new AtomicInteger(0);
    
    /** Table of different replication-behavior objects. */
    private final Map<LogicID, Logic>           logics;
    
    /**
     * 
     * @param dispatcher
     * @param max_q - 0 means infinite.
     * @param queue - the backup of an existing queue.
     */
    public ReplicationStage(SlaveRequestDispatcher dispatcher, int max_q, BlockingQueue<StageRequest> queue) {
        super("ReplicationStage");
        this.q = (queue!=null) ? queue : new PriorityBlockingQueue<StageRequest>();
        this.MAX_Q = max_q;
        this.quit = false;
        this.dispatcher = dispatcher;
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
     * send an request for a stage operation
     *
     * @param rq
     *            the request
     * @param the
     *            method in the stage to execute
     *            
     * @throws TooBusyException
     */
    public void enqueueOperation(int stageOp, Object[] args) throws TooBusyException {
        if (numRequests.incrementAndGet()>MAX_Q && MAX_Q != 0){
            numRequests.decrementAndGet();
            throw new TooBusyException(getName()+": Operation '"+stageOp+"' could not be performed.");
        }      
        q.add(new StageRequest(stageOp, args));
    }

    /**
     * shut the stage thread down
     */
    public void shutdown() {
        if (quit!=true) {
            this.quit = true;
            this.interrupt();
        }
    }

    /**
     * Get current number of requests in the queue.
     *
     * @return queue length
     */
    public int getQueueLength() {
        return q.size();
    }

    /**
     * Needed for resetting the dispatcher.
     * 
     * @return the queue of the replication stage.
     */
    public BlockingQueue<StageRequest> backupQueue() {
        return q;
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
                logics.get(logicID).run();
            } catch(ConnectionLostException cle) {
                Logging.logError(Logging.LEVEL_WARN, this, cle);
                // TODO failover!
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
     */
    public void setLogic(LogicID lgc) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "Replication logic changed: %s", lgc.toString());
        this.logicID = lgc;
    }
    
    /**
     * <p>Frees the given operation and decrements the number of requests.</p>
     * 
     * @param op
     */
    public void finalizeRequest(StageRequest op) {
        op.free();
        int numRqs = numRequests.getAndDecrement();
        assert(numRqs>0) : "The number of requests cannot be negative.";
    }

    /**
     * @return the volatile flag, if this stage shall be terminated.
     */
    public boolean isTerminating() {
        return quit;
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

        public ConnectionLostException(String string) {
            super("Connection to the participant is lost: "+string);
        }
    }
}