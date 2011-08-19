/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.service.logic.BasicLogic;
import org.xtreemfs.babudb.replication.service.logic.LoadLogic;
import org.xtreemfs.babudb.replication.service.logic.Logic;
import org.xtreemfs.babudb.replication.service.logic.Logic.LogicID;
import org.xtreemfs.babudb.replication.service.logic.RequestLogic;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.foundation.LifeCycleThread;

import static org.xtreemfs.babudb.replication.service.logic.Logic.LogicID.*;
import static org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition.*;

/**
 * Stage to perform {@link ReplicationManager}-{@link Operation}s on the slave.
 * 
 * @since 06/08/2009
 * @author flangner
 */

public class ReplicationStage extends LifeCycleThread implements RequestManagement, LockableService {

    private final static int          RETRY_DELAY_MS = 500;
            
    public final AtomicReference<LSN> lastOnView;
        
    /** queue containing all incoming requests */
    private Queue<StageRequest>       q = new PriorityQueue<StageRequest>();
        
    /** set to true if stage should shut down */
    private volatile boolean          quit;
    
    /** used for load balancing */
    private final int                 MAX_Q;
    
    /** mechanism to determine whether BASIC replication is permitted or not */
    private boolean                   locked = false;
    
    /** Table of different replication-behavior objects. */
    private final Map<LogicID, Logic> logics;
       
    /** Listener to wait for a synchronization to finish. */
    private SyncListener              syncListener = null;
    
    /** needed to control the heartbeat */
    private final Pacemaker           pacemaker;
    
    /** interface to {@link BabuDB} internals */
    private final BabuDBInterface     babudb;
    
    private StageCondition            operatingCondition = DEFAULT;
    
    /**
     * @param slaveView
     * @param pacemaker - for the heartbeat.
     * @param max_q - 0 means infinite.
     * @param lastOnView - reference for the compare-variable lastOnView.
     * @param fileIO
     * @param babuInterface
     * @param maxChunkSize
     */
    public ReplicationStage(int max_q, Pacemaker pacemaker, SlaveView slaveView, FileIOInterface fileIO, 
            BabuDBInterface babuInterface, AtomicReference<LSN> lastOnView, int maxChunkSize) {
        
        super("ReplicationStage");

        this.babudb = babuInterface;
        this.lastOnView = lastOnView;
        this.pacemaker = pacemaker;
        this.MAX_Q = max_q;
        
        // ---------------------
        // initialize logic
        // ---------------------
        logics = new HashMap<LogicID, Logic>();
        
        Logic lg = new BasicLogic(babuInterface, slaveView, fileIO, lastOnView, pacemaker);
        logics.put(lg.getId(), lg);
        
        lg = new RequestLogic(babuInterface, slaveView, fileIO, lastOnView);
        logics.put(lg.getId(), lg);
        
        lg = new LoadLogic(babuInterface, slaveView, fileIO, maxChunkSize, lastOnView);
        logics.put(lg.getId(), lg);
    }
    
    
    /* (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public synchronized void start() {
        quit = false;
        super.start();
    }
    
    /**
     * Shut the stage thread down.
     * Resets inner state.
     */
    public synchronized void shutdown() {
        quit = true;
        interrupt();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.RequestManagement#
     *      enqueueOperation(java.lang.Object[])
     */
    @Override
    public synchronized void enqueueOperation(Object[] args) throws BusyServerException {
        
        if ((MAX_Q != 0 && (q.size() + 1) > MAX_Q) || quit) {
            throw new BusyServerException(getName() + ": Operation could not be performed.");
        } else {
            q.add(new StageRequest(args));
        }
        
        // wake-up this stage on first entry
        if (q.size() == 1) {
            notify();
        }
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        
        /* initialization */
        /** Counter for the number of tries needed to perform an operation */
        int tries = 0;
        StageCondition currentCondition;
        
        notifyStarted();
        
        while (!quit) {
            
            try {
                
                // sleep if a request shall be retried 
                if (tries != 0) Thread.sleep(RETRY_DELAY_MS * tries);
               
                StageRequest rq = null;
                synchronized(this) {
                    
                    // wait for requests to become available for processing
                    // or the operatingCondition to change
                    while ((operatingCondition.equals(DEFAULT) && q.isEmpty()) || 
                           (locked && operatingCondition.equals(LOCKED))) {
                        wait();
                    }
                                        
                    // get the processing information
                    currentCondition = operatingCondition;
                    if (locked && currentCondition.equals(DEFAULT)) {
                        currentCondition = LOCKED;
                        Thread.sleep(RETRY_DELAY_MS);
                    } else if (currentCondition.equals(DEFAULT)) {
                        rq = q.poll();
                        assert(rq != null);
                    }
                }
                
                currentCondition = process(currentCondition, rq);
                
                // thread-safety for the operatingCondition
                synchronized (this) {
                    if (!currentCondition.equals(LOCKED) && !currentCondition.equals(DEFAULT) && 
                        currentCondition.equals(operatingCondition)) {
                        
                        // -> clean restart
                        if (++tries < ReplicationConfig.MAX_RETRIES) {
                            
                            // cancel syncListener
                            if (syncListener != null) {
                                syncListener.failed(new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                                        "Synchronization was canceled after " + ReplicationConfig.MAX_RETRIES + 
                                        " retries."));
                                syncListener = null;
                            }
                            
                            // reset stage condition
                            clearQueue();
                            operatingCondition = StageCondition.DEFAULT;
                        }
                    } else {
                        // reset the number of tries
                        tries = 0;
                        // update condition
                        operatingCondition = currentCondition; 
                    }
                }
            } catch(InterruptedException ie) {
                if (!quit) {
                    clearQueue();
                    notifyCrashed(ie);
                    return;
                } 
            } 
        }
        
        clearQueue();
        notifyStopped();
    }
    
    private StageCondition process(StageCondition current, StageRequest rq) throws InterruptedException {
        StageCondition result;
        
        if (current.equals(LOCKED)) {
            result = current;
        } else if (current.equals(DEFAULT)) {
            
            assert (rq != null);
            
            // update the heartbeatThread and re-animate
            if (pacemaker.hasInfarct()) {
                pacemaker.updateLSN(babudb.getState());
                pacemaker.reanimate();
            }
                            
            // run DEFAULT-logic
            result = logics.get(BASIC).run(rq);
            if (!result.equals(DEFAULT)) {
                synchronized (this) {
                    q.add(rq);
                }
            }
        } else {
            
            assert (rq == null);
            
            // prevent the heartbeatThread from sending messages as long as
            // this server is synchronizing with another server
            pacemaker.infarction();
            
            // run sync-logic
            result = logics.get(current.logicID).run(current);
        }
        
        return result;
    }
    
    /**
     * Method to trigger a {@link LOAD} manually.
     * Listener will contain true on finish , if it was a request synchronization,
     * or if just the log file was switched. 
     * otherwise, if it was a load, the listener will be notified with false.
     * 
     * @param syncListener
     * @param to
     */
    public synchronized void manualLoad(SyncListener syncListener, LSN to) {
                
        LSN start = babudb.getState();
        
        if (start.equals(to)) {
            
            syncListener.synced(to);
        } else {
        
            this.syncListener = syncListener;
            LSN end = new LSN(
                        // we have to assume, that there is an entry following the
                        // state we would like to establish, because the request
                        // logic will always exclude the 'end' of this range
                        to.getViewId(), to.getSequenceNo() + 1L);
            
            // update local DB state
            StageCondition old = operatingCondition;
            if (start.compareTo(end) < 0) {
                operatingCondition = new StageCondition(end);
                
            // roll-back local DB state
            } else {
                operatingCondition = new StageCondition(LOAD, end);
            }
            
            // necessary to wake up the mechanism
            if ((old.equals(DEFAULT) && q.isEmpty()) || 
                (locked && old.equals(LOCKED))) {
                notify();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.RequestManagement#
     *      createStableState(org.xtreemfs.babudb.lsmdb.LSN, java.net.InetSocketAddress, 
     *                        org.xtreemfs.babudb.replication.control.ControlLayerInterface)
     */
    @Override
    public void createStableState(final LSN lastLSNOnView, final InetSocketAddress master, 
            final ControlLayerInterface control) throws InterruptedException {
        
        // if slave already is in a stable DB state, unlock user operations immediately and return
        if (lastOnView.get().equals(lastLSNOnView)) {
            
            try {
                
                // only take a checkpoint if the master did so
                if (lastLSNOnView.getSequenceNo() != 0L) {
                    lastOnView.set(babudb.checkpoint());
                    pacemaker.updateLSN(babudb.getState());
                }
            } catch (BabuDBException e) {
                
                
                // taking the checkpoint was interrupted, the only reason for that is a 
                // crash, or a shutdown. either way these symptoms are treated like a crash 
                // of the replication manager.
                assert (e.getErrorCode() == ErrorCode.INTERRUPTED);
                
                notifyCrashed(e);
            }
            
        // otherwise establish a stable global DB state
        } else {
            
            manualLoad(new SyncListener() {
                
                @Override
                public void synced(LSN lsn) {
                    try {
                        
                        // always take a checkpoint
                        lastOnView.set(babudb.checkpoint());
                        pacemaker.updateLSN(babudb.getState());
                        
                    } catch (BabuDBException e) {
                        
                        // taking the checkpoint was interrupted, the only reason for that is a 
                        // crash, or a shutdown. either way these symptoms are treated like a crash 
                        // of the replication manager.
                        
                        assert (e.getErrorCode() == ErrorCode.INTERRUPTED);
                        
                        notifyCrashed(e);
                    }
                }
                
                @Override
                public void failed(Exception ex) {
                    // manual load failed. retry if master has not changed meanwhile
                    // ignore the failure otherwise
                    try {
                        if (master.equals(control.getLeaseHolder(-1))) {
                            createStableState(lastLSNOnView, master, control);
                        }
                    } catch (InterruptedException e) {
                        /* ignored */
                    } 
                }
            }, lastLSNOnView);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LockableService#lock()
     */
    @Override
    public synchronized void lock() throws InterruptedException {
        if (!locked) {
            locked = true;
            logics.get(BASIC).waitForSteadyState();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.LockableService#unlock()
     */
    @Override
    public synchronized void unlock() {
        if (locked) {
            locked = false;
            if (operatingCondition.equals(LOCKED)) {
                operatingCondition = DEFAULT;
                notify();
            }
        }
    }
    
/*
 * private methods
 */
    
    /**
     * Takes the requests from the queue and frees there buffers.
     */
    private void clearQueue() {
        StageRequest rq = q.poll();
        while (rq != null) {
            rq.free();
            rq = q.poll();
        }
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
            super("Participant is too busy at the moment: " + string);
        }
    }
    
    /**
     * Request for filling a loophole.
     * 
     * @author flangner
     * @since 01/05/2011
     */
    public static final class StageCondition {
        
        public final static StageCondition DEFAULT = new StageCondition(BASIC, null);
        public final static StageCondition LOAD_LOOPHOLE = new StageCondition(LOAD, null);
        public final static StageCondition LOCKED = null;
        public final LSN end;
        public final LogicID logicID;
                
        /**
         * Marks a loophole that has to fixed by a REQUEST.
         * 
         * @param end
         */
        public StageCondition(LSN end) {
            this(REQUEST, end);
        }
        
        /**
         * User-defined LoopholeMarker.
         * 
         * @param logicID
         * @param end
         */
        public StageCondition(LogicID logicID, LSN end) {
            assert (logicID != null);
            
            this.logicID = logicID;
            this.end = end;   
        }
    }
}