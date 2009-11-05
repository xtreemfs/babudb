/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.SimplifiedBabuDBRequestListener;
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.ReplicateOperation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.babudb.replication.stages.HeartbeatThread;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Dispatches incoming master-requests.
 * 
 * @since 05/03/2009
 * @author flangner
 * @see RequestDispatcher
 */

public class SlaveRequestDispatcher extends RequestDispatcher {
        
    public MasterClient     master;
    
    /*
     * stages
     */
    public ReplicationStage replication;
    public HeartbeatThread  heartbeat;
    
    /**
     * Initial setup. The master will not be registered at this point.
     * 
     * @param dbs
     * @param state - to start with.
     * @throws IOException
     */
    public SlaveRequestDispatcher(BabuDB dbs, DispatcherState state) 
        throws IOException {
        
        super("Slave", dbs);
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, ((ReplicationConfig) dbs.
                getConfig()).getMaxQueueLength(), null, state.latest);   
        this.heartbeat = new HeartbeatThread(this, state.latest);
    }
    
    /**
     * Constructor to change the {@link RequestDispatcher} behavior to
     * master using the old dispatcher.
     * 
     * @param oldDispatcher
     */
    public SlaveRequestDispatcher(RequestDispatcher oldDispatcher) {
        super("Slave", oldDispatcher);
        assert(oldDispatcher.isPaused());
        
        DispatcherState state = oldDispatcher.getState();
        LSN latest = state.latest;
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, ((ReplicationConfig) dbs.
                getConfig()).getMaxQueueLength(), state.requestQueue, latest);   
        this.heartbeat = new HeartbeatThread(this, latest);
    }
    
    /**
     * Coin the {@link SlaveRequestDispatcher} on the given master.
     * 
     * @param master
     */
    public void coin(InetSocketAddress master) {
        
        // --------------------------
        // register the master client
        // --------------------------
        this.master = new MasterClient(rpcClient, master);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#shutdown()
     */
    @Override
    public void shutdown() {
        if (!isPaused()) {
            try {
                replication.shutdown();
                heartbeat.shutdown();          
                
                replication.waitForShutdown();
                heartbeat.waitForShutdown();  
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
            }
        }
        super.shutdown();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        if (!isPaused()) {
            replication.shutdown();
            heartbeat.shutdown();
        }
        super.asyncShutdown();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#initializeOperations()
     */
    @Override
    protected void initializeOperations() {
        Operation op = new StateOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new ReplicateOperation(this);
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
     * @param from
     * @param to
     * @throws InterruptedException 
     * 
     * @return false, if no additional snapshot has to be taken, true otherwise.
     */
    public boolean synchronize(LSN from, LSN to) throws InterruptedException{
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Starting synchronization " +
        		"from '%s' to '%s'.", from.toString(), to.toString());
        
        assert(!isPaused()) : "The Replication may not be stopped before!";
        boolean result;
        
        final AtomicBoolean ready = new AtomicBoolean(false);
        result = replication.manualLoad(new SimplifiedBabuDBRequestListener() {
        
            @Override
            public void finished(BabuDBException error) {
                synchronized (ready) {
                    ready.set(true);
                    ready.notify();
                }
            }
        }, from, to);
        
        synchronized (ready) {
            if (!ready.get())
                ready.wait();
            
            ready.set(false);
        }
        
        pauses(new SimplifiedBabuDBRequestListener() {
        
            @Override
            public void finished(BabuDBException error) {
                synchronized (ready) {
                    ready.set(true);
                    ready.notify();
                }
            }
        });
        
        synchronized (ready) {
            if (!ready.get())
                ready.wait();
        }
        
        assert(to.equals(getState().latest)) : "Synchronization failed: "
            +to.toString()+" != "+getState().latest;
        
        return result;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#getState()
     */
    @Override
    public DispatcherState getState() {
        return new DispatcherState(dbs.getLogger().getLatestLSN(), 
                replication.backupQueue());
    }
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#pauses(org.xtreemfs.babudb.SimplifiedBabuDBRequestListener)
     */
    @Override
    public void pauses(SimplifiedBabuDBRequestListener listener) {
        if (!isPaused()) {
            try {
                replication.shutdown();
                heartbeat.shutdown();          
                
                if (listener != null) {
                    replication.waitForShutdown();
                    heartbeat.waitForShutdown();
                }
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "could not stop the dispatcher");
            }
        }
        super.pauses(listener);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#continues(org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState)
     */
    @Override
    public void continues(IState state) {
        if (this.master == null) throw new UnsupportedOperationException(
                "Cannot continue the replication, while it was not coined." +
                "Use coin() instead!");
  
        try {
            replication.start();
            heartbeat.start();
            
            replication.waitForStartup();
            heartbeat.waitForStartup();
        } catch (Exception ex) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "startup failed");
            Logging.logMessage(Logging.LEVEL_ERROR, this, ex.getMessage());
            System.exit(1);
        }
        super.continues(state);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#_replicate(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.include.common.buffer.ReusableBuffer)
     */
    @Override
    protected ReplicateResponse _replicate(LogEntry le, ReusableBuffer payload)
            throws NotEnoughAvailableSlavesException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#subscribeListener(org.xtreemfs.babudb.replication.LatestLSNUpdateListener)
     */
    @Override
    public void subscribeListener(LatestLSNUpdateListener listener) {
        throw new UnsupportedOperationException();
    }
}
