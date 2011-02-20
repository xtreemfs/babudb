/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.replication.control.ControlLayer;
import org.xtreemfs.babudb.replication.service.ServiceLayer;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.babudb.replication.transmission.TransmissionLayer;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.LifeCycleListener;
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

public class ReplicationManager implements LifeCycleListener {
        
    public final static String  VERSION = "1.0.0 (v1.0 RC1)";
    
    private final TopLayer      controlLayer;
    private final ServiceLayer  serviceLayer;
    private final Layer         transmissionLayer;
        
    /**
     * <p>For setting up the {@link BabuDB} with replication. 
     * Replication instance will be remaining stopped and in slave-mode.</p>
     * 
     * @param dbs
     * @param conf
     * @throws Exception 
     */
    public ReplicationManager(BabuDBInternal dbs, ReplicationConfig conf) throws Exception {
        
        TimeSync.initializeLocal(conf.getTimeSyncInterval(), 
                                 conf.getLocalTimeRenew()).setLifeCycleListener(this);

        TransmissionLayer t = new TransmissionLayer(conf);
        serviceLayer = new ServiceLayer(conf, new BabuDBInterface(dbs), t);
        ControlLayer c = new ControlLayer(serviceLayer, conf);
        serviceLayer.init(c);
        c.registerReplicationInterface(serviceLayer.getLockableService());
        
        transmissionLayer = t;
        controlLayer = c;
        
        transmissionLayer.setLifeCycleListener(this);
        serviceLayer.setLifeCycleListener(this);
        controlLayer.setLifeCycleListener(this);
    }
    
/*
 * internal interface for BabuDB
 */
    
    /**
     * Starts the stages if available.
     */
    public void initialize(LockableService babudbProxy) {
        
        assert (babudbProxy != null);
        
        controlLayer.registerUserInterface(babudbProxy);
        controlLayer.start();
        serviceLayer.start(controlLayer);
        transmissionLayer.start();
    }

    /**
     * @return the currently designated master, or null, if BabuDB is 
     *         suspended at the moment.
     */
    public InetSocketAddress getMaster() {
        return controlLayer.getLeaseHolder();
    }
    
    /**
     * @param address - the address to compare with.
     * @return true, if the given address is the address of this server. false otherwise.
     */
    public boolean isItMe(InetSocketAddress address) {
        return controlLayer.isItMe(address);
    }
    
    /**
     * <p>
     * Approach for a Worker to announce a new {@link LogEntry} 
     * <code>le</code> to the {@link ReplicationThread}.
     * </p>
     * 
     * @param le - the original {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    public ReplicateResponse replicate(LogEntry le) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                "Performing requests: replicate...");
        
        return serviceLayer.replicate(le);
    }
    
    /**
     * <p>
     * Registers the listener for a replicate call.
     * </p>
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse response) {
        serviceLayer.subscribeListener(response);
    }

    /**
     * <p>
     * Stops the replication process by shutting down the dispatcher.
     * And resetting the its state.
     * </p>
     */
    public void shutdown() throws Exception {
        controlLayer.shutdown();
        serviceLayer.shutdown();
        transmissionLayer.shutdown();
        TimeSync.getInstance().shutdown();
    }

    /**
     * @return a client to remotely access the BabuDB with master-privilege.
     */
    public RemoteAccessClient getRemoteAccessClient() {
        return ((TransmissionLayer) transmissionLayer).getRemoteAccessClient();
    }
    
/*
 * LifeCycleListener for the TimeSync-Thread
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable exc) {
        Logging.logMessage(Logging.LEVEL_CRIT, this, 
                "An essential replication component has crashed, because %s.",
                exc.getMessage());
        Logging.logError(Logging.LEVEL_CRIT, this, exc);
        
        controlLayer.asyncShutdown();
        serviceLayer.asyncShutdown();
        transmissionLayer.asyncShutdown();
        throw new RuntimeException(exc);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {}

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {}

}
