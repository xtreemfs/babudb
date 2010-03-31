/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicateResponse;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>Implements the {@link ReplicationManager} user interface.</p>
 * <p>Configurable settings.</p>
 * <p>Used to be one single Instance.</p>
 * <p>Thread safe.</p>
 * 
 * @since 06/05/2009
 * @author flangner
 */

public class ReplicationManagerImpl implements ReplicationManager {
        
    private ReplicationControlLayer     replicationControlLayer;
    
    private volatile boolean            slaveCheck = false;
    
    /**
     * <p>For setting up the {@link BabuDB} with replication. 
     * Replication instance will be remaining stopped and in slave-mode.</p>
     * 
     * @param dbs
     * @param initial
     * @throws Exception 
     */
    public ReplicationManagerImpl(BabuDB dbs, LSN initial) 
        throws Exception {
        ReplicationConfig conf = (ReplicationConfig) dbs.getConfig();
        TimeSync.initializeLocal(conf.getTimeSyncInterval(), 
                conf.getLocalTimeRenew()).setLifeCycleListener(this);
        this.replicationControlLayer = new ReplicationControlLayer(dbs, initial);
    }
    
    /**
     * <p>For setting up the {@link BabuDB} with replication. 
     * Replication instance will be remaining stopped and in slave-mode.</p>
     * 
     * @param dbs
     * @param initial
     * @param fleaseStage - can be used if the application that uses 
     *                  {@link BabuDB} has {@link Flease} in common with it, to
     *                  avoid duplication.
     * @throws Exception 
     */
    public ReplicationManagerImpl(BabuDB dbs, LSN initial, 
        FleaseStage fleaseStage) throws Exception {
        ReplicationConfig conf = (ReplicationConfig) dbs.getConfig();
        TimeSync.initializeLocal(conf.getTimeSyncInterval(), 
                conf.getLocalTimeRenew()).setLifeCycleListener(this);
        this.replicationControlLayer = new ReplicationControlLayer(dbs, initial, 
                fleaseStage);
    }
    
/*
 * interface
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#getMaster()
     */
    @Override
    public InetSocketAddress getMaster() {
        return this.replicationControlLayer.getLeaseHolder();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#getStates(java.util.List)
     */
    @Override
    public Map<InetSocketAddress,LSN> getStates(List<InetSocketAddress> babuDBs) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Performing requests: state...");
        
        return replicationControlLayer.getStates(babuDBs);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#isMaster()
     */
    @Override
    public boolean isMaster() {
        try {
            return replicationControlLayer.hasLease();
        } catch (InterruptedException e) {
            Logging.logMessage(Logging.LEVEL_INFO, this, "IsMaster-method has" +
            		" been interrupted, a failover may be still in progress" +
            		", because: %s", e.getMessage());
            return false;
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#shutdown()
     */
    @Override
    public void shutdown() throws Exception {
        replicationControlLayer.shutdown();
        TimeSync.getInstance().shutdown();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#halt()
     */
    @Override
    public void manualFailover() {
        replicationControlLayer.handoverLease("Handover was triggered manually.");
    }
    
/*
 * internal interface for BabuDB
 */
    
    /**
     * <p>Approach for a Worker to announce a new {@link LogEntry} <code>le</code> to the {@link ReplicationThread}.</p>
     * 
     * @param le - the original {@link LogEntry}.
     * @param buffer - the serialized {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Performing requests: replicate...");
        
        return replicationControlLayer.replicate(le, buffer);
    }
    
    /**
     * Starts the stages if available.
     * @throws BabuDBException 
     */
    public void initialize() throws BabuDBException {
        replicationControlLayer.start();
        slaveCheck = true;
    }
    
    /**
     * <p>
     * Registers the listener for a replicate call.
     * </p>
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse response) {
        replicationControlLayer.subscribeListener(response);
    }
    
    /**
     * Reports the last-on-view LSN to the replication-mechanism, 
     * after taking a checkpoint.
     * 
     * @param lsn
     */
    public void updateLastOnView(LSN lsn) {
        replicationControlLayer.updateLastOnView(lsn);
    }
    
    /**
     * @return true if the replication mechanism already has been initialized,
     *         false otherwise.
     */
    public boolean isInitialized() {
        return slaveCheck;
    }
    
/*
 * LifeCycleListener for the TimeSync-Thread
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable exc) {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "TimeSync crashed, because: "
                + exc.getMessage());
        this.replicationControlLayer.handoverLease(exc);
        try {
            this.replicationControlLayer.shutdown();
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_ERROR, this, e);
        }
        throw new RuntimeException(exc);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        Logging.logMessage(Logging.LEVEL_INFO, this, "TimeSync successfully %s.", "stopped");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        Logging.logMessage(Logging.LEVEL_INFO, this, 
                "TimeSync successfully started. System Time: %d", 
                TimeSync.getLocalSystemTime());
    }
}
