/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.ReplicateOperation;
import org.xtreemfs.babudb.replication.stages.HeartbeatThread;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Dispatches incoming master-requests.
 * 
 * @since 05/03/2009
 * @author flangner
 * @see RequestDispatcher
 */

public class SlaveRequestDispatcher extends RequestDispatcher {
        
    public final MasterClient master;
    
    /*
     * stages
     */
    public final ReplicationStage replication;
    public final HeartbeatThread  heartbeat;
        
    /**
     * Constructor to change the {@link RequestDispatcher} behavior to
     * master using the old dispatcher.
     * 
     * @param oldDispatcher
     * @param master
     */
    public SlaveRequestDispatcher(RequestDispatcher oldDispatcher, 
            InetSocketAddress master, LifeCycleListener listener) {
        
        super("Slave", oldDispatcher);
        this.timeDriftDetector.stop();
        
        oldDispatcher.suspend();
        DispatcherState state = oldDispatcher.getState();
        LSN latest = state.latest;
        
        // --------------------------
        // register the master client
        // --------------------------
        this.master = new MasterClient(rpcClient, master, 
                getConfig().getInetSocketAddress());
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        this.replication = new ReplicationStage(this, ((ReplicationConfig) dbs.
                getConfig()).getMaxQueueLength(), state.requestQueue, latest);
        this.replication.setLifeCycleListener(listener);
        this.heartbeat = new HeartbeatThread(this, latest);
        this.heartbeat.setLifeCycleListener(listener);
        this.replication.start();
        this.heartbeat.start();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#shutdown()
     */
    @Override
    public void shutdown() {
        suspend();
        super.shutdown();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        if (!suspended.get()) {
            replication.shutdown();
            heartbeat.shutdown();
        }
        super.asyncShutdown();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#additionalOperations(java.util.Map)
     */
    @Override
    protected void additionalOperations(Map<Integer, Operation> operations) {
        Operation op = new ReplicateOperation(this);
        operations.put(op.getProcedureId(),op);
    }
    
   /**
     * Updates the {@link LSN} identifying the last written {@link LogEntry}.
     * 
     * @param lsn
     */
    public synchronized void updateLatestLSN(LSN lsn) {
        heartbeat.updateLSN(lsn);
    }

    /**
     * Performs a load on the master to synchronize with the latest state.
     * 
     * @param to
     * @throws BabuDBException if synchronization failed. 
     * 
     * @return false, if no additional snapshot has to be taken, true otherwise.
     */
    public boolean synchronize(LSN to) throws BabuDBException {
        LSN from = getState().latest;
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "Starting synchronization" +
        		" from '%s' to '%s'.", from.toString(), to.toString());
        
        boolean result;
        final BabuDBRequest<Boolean> ready = new BabuDBRequest<Boolean>();
        replication.manualLoad(ready, from, to);
        result = ready.get();
        
        assert(to.equals(getState().latest)) : "Synchronization failed: " +
            "(expected=" + to.toString() + ") != (acknowledged=" + 
            getState().latest + ")";
        
        return result;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#getState()
     */
    @Override
    public DispatcherState getState() {
        BlockingQueue<StageRequest> q = null;
        if (replication != null) q = replication.backupQueue();
        
        return new DispatcherState(dbs.getLogger().getLatestLSN(), q);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#_suspend()
     */
    @Override
    public void _suspend() {
        try {
            replication.shutdown();
            heartbeat.shutdown();          
            
            replication.waitForShutdown();
            heartbeat.waitForShutdown();
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "could not stop the" +
            		" dispatcher");
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#replicate(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    ReplicateResponse _replicate(LogEntry le, ReusableBuffer payload) {
        return new ReplicateResponse(le, new UnsupportedOperationException(
        		"The dispatcher is running in slave-mode and for that" +
        		" could not process the given request."));
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#subscribeListener(org.xtreemfs.babudb.replication.LatestLSNUpdateListener)
     */
    @Override
    public void subscribeListener(LatestLSNUpdateListener listener) {
        listener.failed();
    }
}
