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
        this.isMaster = false;
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, ((ReplicationConfig) dbs.
                getConfig()).getMaxQ(), null, state.latest);   
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
        LSN latest = oldDispatcher.getState().latest;
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, ((ReplicationConfig) dbs.
                getConfig()).getMaxQ(), null, latest);   
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
        this.stopped = false;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#start()
     */
    @Override
    public void start() {
        super.start();
        this.stopped = true;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#shutdown()
     */
    @Override
    public void shutdown() {
        if (!stopped) {
            try {
                replication.shutdown();
                heartbeat.shutdown();          
                
                replication.waitForShutdown();
                heartbeat.waitForShutdown();  
                
                replication.clearQueue();
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
        replication.shutdown();
        heartbeat.shutdown();
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
     * @throws BabuDBException 
     * @throws InterruptedException 
     */
    public void synchronize(LSN from, LSN to) throws BabuDBException, InterruptedException{
        assert(!stopped) : "The Replication may not be stopped before!";
        
        final AtomicBoolean ready = new AtomicBoolean(false);
        replication.manualLoad(new SimplifiedBabuDBRequestListener() {
        
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
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#getState()
     */
    @Override
    public DispatcherState getState() {
        return new DispatcherState(dbs.getLogger().getLatestLSN(), replication.backupQueue());
    }
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#pauses(org.xtreemfs.babudb.SimplifiedBabuDBRequestListener)
     */
    @Override
    public void pauses(SimplifiedBabuDBRequestListener listener) {
        if (!stopped) {
            try {
                replication.shutdown();
                heartbeat.shutdown();          
                
                replication.waitForShutdown();
                heartbeat.waitForShutdown();  
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
    public void continues(DispatcherState state) throws BabuDBException {
        if (this.master == null) throw new UnsupportedOperationException(
                "Cannot continue the replication, while it was not coined." +
                "Use coin() instead!");
        
        this.replication = new ReplicationStage(this,configuration.getMaxQ(),state.requestQueue,state.latest);
        this.heartbeat = new HeartbeatThread(this,state.latest);
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
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#_replicate(org.xtreemfs.babudb.log.LogEntry)
     */
    @Override
    protected void _replicate(LogEntry le)
            throws NotEnoughAvailableSlavesException, InterruptedException {
        throw new UnsupportedOperationException();
    }
}
