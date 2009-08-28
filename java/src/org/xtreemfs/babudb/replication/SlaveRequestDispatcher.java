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
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.ReplicateOperation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.babudb.replication.stages.HeartbeatThread;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
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
        
    public final SlaveConfig        configuration;
    public final MasterClient       master;
    
    /*
     * stages
     */
    public final ReplicationStage   replication;
    public final HeartbeatThread    heartbeat;
    
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
    public SlaveRequestDispatcher(SlaveConfig config, BabuDB dbs, DispatcherBackupState backupState) throws IOException {
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

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#getLatestLSN()
     */
    @Override
    public LSN getLatestLSN() {
        return dbs.getLogger().getLatestLSN();
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#stop()
     */
    @Override
    public DispatcherBackupState stop() {
        try {
            replication.shutdown();
            heartbeat.shutdown();          
            
            replication.waitForShutdown();
            heartbeat.waitForShutdown();  
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "could not stop the dispatcher");
        }
        super.shutdown();
        
        // spinLock to wait for the logger-queue to run empty
        try {
            while (dbs.getLogger().getQLength() != 0)
                Thread.sleep(100);
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
        
        return new DispatcherBackupState(getLatestLSN(), replication.backupQueue());
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#replicate(org.xtreemfs.babudb.log.LogEntry)
     */
    @Override
    protected void replicate(LogEntry le)
            throws NotEnoughAvailableSlavesException, InterruptedException {
        throw new UnsupportedOperationException();
    } 
}
