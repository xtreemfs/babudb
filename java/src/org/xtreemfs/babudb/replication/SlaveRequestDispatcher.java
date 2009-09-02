/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;

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
import org.xtreemfs.babudb.replication.stages.logic.LogicID;
import org.xtreemfs.include.common.config.SlaveConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Dispatches incoming master-requests.
 * 
 * @since 05/03/2009
 * @author flangner
 * @see RequestDispatcher
 */

public class SlaveRequestDispatcher extends RequestDispatcher {
        
    public final SlaveConfig    configuration;
    public final MasterClient   master;
    
    /*
     * stages
     */
    public ReplicationStage     replication;
    public HeartbeatThread      heartbeat;
    
    /**
     * Initial setup.
     * 
     * @param config
     * @param dbs
     * @param initial
     * @throws IOException
     */
    public SlaveRequestDispatcher(SlaveConfig config, BabuDB dbs, LSN initial) throws IOException {
        super("Slave", config, dbs);
        this.isMaster = false;
        this.configuration = config;
                
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, config.getMaxQ(), null, initial);   
        this.heartbeat = new HeartbeatThread(this, initial);
        
        // --------------------------
        // register the master client
        // --------------------------
        
        this.master = new MasterClient(rpcClient,config.getMaster());
    }
    
    /**
     * Reset configuration.
     * 
     * @param config
     * @param dbs
     * @param backupState - needed if the dispatcher shall be reset. Includes the initial LSN.
     * @throws IOException
     */
    public SlaveRequestDispatcher(SlaveConfig config, BabuDB dbs, DispatcherState backupState) throws IOException {
        super("Slave", config, dbs);
        this.configuration = config;
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, config.getMaxQ(), backupState.requestQueue, backupState.latest);   
        this.heartbeat = new HeartbeatThread(this, backupState.latest);
        
        // --------------------------
        // register the master client
        // --------------------------
        
        this.master = new MasterClient(rpcClient,config.getMaster());
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#start()
     */
    @Override
    public void start() {
        super.start();
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
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#shutdown()
     */
    @Override
    public void shutdown() {
        try {
            replication.shutdown();
            heartbeat.shutdown();          
            
            replication.waitForShutdown();
            heartbeat.waitForShutdown();  
            
            replication.clearQueue();
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
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
     * @param lsn
     */
    public void synchronize(LSN lsn){
        replication.setLogic(LogicID.LOAD, "Manual synchronization to LSN "+lsn.toString());
        // TODO wait for load-finish
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
        try {
            replication.shutdown();
            heartbeat.shutdown();          
            
            replication.waitForShutdown();
            heartbeat.waitForShutdown();  
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "could not stop the dispatcher");
        }
        super.pauses(listener);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#continues(org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState)
     */
    @Override
    public void continues(DispatcherState state) throws BabuDBException {
        this.replication = new ReplicationStage(this,configuration.getMaxQ(),state.requestQueue,state.latest);
        this.heartbeat = new HeartbeatThread(this,state.latest);
        this.start();
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
