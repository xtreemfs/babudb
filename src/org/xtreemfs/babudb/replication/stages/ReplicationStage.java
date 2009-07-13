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
import java.util.concurrent.atomic.AtomicReference;

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
    
    /** queue containing all incoming requests */
    private final BlockingQueue<StageRequest>      q;
    
    /** missingRange shared between PLAIN and REQUEST {@link Logic} */
    private final AtomicReference<LSNRange> missing = new AtomicReference<LSNRange>(null);
    
    /** {@link LogicID} of the {@link Logic} currently used. */
    private final AtomicReference<LogicID> currentLogic = new AtomicReference<LogicID>(BASIC);
    
    /** set to true if stage should shut down */
    private volatile boolean                quit;
    
    /** used for load balancing */
    private final int                       MAX_Q;
    private final AtomicInteger             numRequests = new AtomicInteger(0);
    
    /** Table of different replication-behavior objects. */
    private final Map<LogicID, Logic>       logics;
    
    /**
     * 
     * @param dispatcher
     * @param max_q - 0 means infinite.
     */
    public ReplicationStage(SlaveRequestDispatcher dispatcher, int max_q) {
        super("ReplicationStage");
        this.MAX_Q = max_q;
        this.q = new PriorityBlockingQueue<StageRequest>();
        this.quit = false;
        this.dispatcher = dispatcher;
        setLifeCycleListener(dispatcher);
        
        // ---------------------
        // initialize logic
        // ---------------------
        logics = new HashMap<LogicID, Logic>();
        
        Logic lc = new BasicLogic(this,q);
        logics.put(lc.getId(), lc);
        
        lc = new RequestLogic(this);
        logics.put(lc.getId(), lc);
        
        lc = new LoadLogic(this);
        logics.put(lc.getId(), lc);
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
            throw new TooBusyException("Operation '"+stageOp+"' could not be performed at the "+getName()+", because it is too busy.");
        }      
        q.add(new StageRequest(stageOp, args));
    }

    /**
     * shut the stage thread down
     */
    public void shutdown() {
        this.quit = true;
        this.interrupt();
    }

    /**
     * Get current number of requests in the queue.
     *
     * @return queue length
     */
    public int getQueueLength() {
        return q.size();
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
                logics.get(currentLogic.get()).run();
            } catch(Exception e) {
                Logging.logError(Logging.LEVEL_ERROR, this, e);
                notifyCrashed(e);
                quit = true;
            }
        }
        notifyStopped();
    }

    /**
     * Sets the {@link Logic} to use.
     * 
     * @param logic
     * @return true, if logic could be set - no incompatible logic was set before.
     */
    public boolean setCurrentLogic(LogicID logic) {
        if (logic.equals(REQUEST))
            return currentLogic.compareAndSet(BASIC, logic);
        else {
            missing.set(null);
            currentLogic.set(logic);
            return true;
        }
    }
    
    /**
     * @param missing the missingRange to set
     * @return true, if range could be set - no other range is requested.
     */
    public boolean setMissing(LSNRange missing) {
        return this.missing.compareAndSet(null, missing);
    }

    /**
     * @return the missing
     */
    public LSNRange getMissing() {
        return this.missing.get();
    }
    
    /**
     * Frees the given operation and decrements the number of requests.
     * 
     * @param op
     */
    public void finalizeRequest(StageRequest op) {
        op.free();
        assert(numRequests.getAndDecrement()>0) : "The number of requests cannot be negative.";
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
            super(string);
        }
    }
}