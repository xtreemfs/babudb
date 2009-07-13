/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;

import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.CopyOperation;
import org.xtreemfs.babudb.replication.operations.CreateOperation;
import org.xtreemfs.babudb.replication.operations.DeleteOperation;
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
     * @param initial
     * @param db
     * @throws IOException
     */
    public SlaveRequestDispatcher(SlaveConfig config, BabuDBImpl db, LSN initial) throws IOException {
        super("Slave", config, db);
        this.configuration = config;
        
        // --------------------------
        // initialize internal stages
        // --------------------------
        
        this.replication = new ReplicationStage(this, config.getMaxQ());   
        this.heartbeat = new HeartbeatThread(this, initial);
        
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
        super.shutdown();
        try {
            replication.shutdown();
            heartbeat.shutdown();          
            
            replication.waitForShutdown();
            heartbeat.waitForShutdown();
        } catch (Exception e) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "shutdown failed");
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        super.asyncShutdown();
        replication.shutdown();
        heartbeat.shutdown();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#initializeOperations()
     */
    @Override
    protected void initializeOperations() {
        Operation op = new StateOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new CreateOperation(this);
        operations.put(op.getProcedureId(),op);

        op = new CopyOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new DeleteOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new ReplicateOperation(this);
        operations.put(op.getProcedureId(),op);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#initializeEvents()
     */
    @Override
    protected void initializeEvents() {
        /* no events registered at the moment */      
    }

    /**
     * Updates the {@link LSN} identifying the last written {@link LogEntry}.
     * 
     * @param lsn
     */
    public synchronized void updateLatestLSN(LSN lsn) {
        heartbeat.updateLSN(lsn);
    }
}
