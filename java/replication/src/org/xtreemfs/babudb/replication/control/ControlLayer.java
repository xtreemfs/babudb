/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.TopLayer;
import org.xtreemfs.babudb.replication.service.ServiceToControlInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestControl;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Contains the control logic for steering the replication process.
 * 
 * @author flangner
 * @since 02/24/2010
 */
public class ControlLayer extends TopLayer {
    
    /** designation of the flease-cell */
    private final static ASCIIString        REPLICATION_CELL = new ASCIIString("replication");
    
    /** always access {@link Flease} from here */
    private final FleaseStage               fleaseStage;
    
    private final List<InetSocketAddress>   fleaseParticipants;
    
    /**
     * component to ensure {@link Flease}'s requirement of loosely synchronized
     * clocks
     */
    private final TimeDriftDetector         timeDriftDetector;
    
    /** interface to the underlying layer */
    private final ServiceToControlInterface serviceInterface;
    
    /** the local address used for the net-communication */
    private final InetSocketAddress         thisAddress;
    
    /** listener and storage for the up-to-date lease informations */
    private final FleaseHolder              leaseHolder;
    
    /** thread to execute failover requests */
    private final FailoverTaskRunner        failoverTaskRunner;
    
    /** services that have to be locked during failover */
    private LockableService                 userInterface;
    private LockableService                 replicationInterface;
    private RequestControl                  proxyRequestCtrl;
    
    /** 
     * to ensure services not to be locked after unlock this flag tracks the currently registered 
     * master 
     */
    private AtomicReference<InetSocketAddress> masterLock = 
        new AtomicReference<InetSocketAddress>(null);
    
    private final AtomicBoolean initialFailoverObserved = new AtomicBoolean(false);
    
    public ControlLayer(ServiceToControlInterface serviceLayer, ReplicationConfig config) 
            throws IOException {
        
        // ----------------------------------
        // initialize the time drift detector
        // ----------------------------------
        timeDriftDetector = new TimeDriftDetector(this, 
                serviceLayer.getParticipantOverview().getConditionClients(), 
                config.getLocalTimeRenew());
        
        // ----------------------------------
        // initialize the replication 
        // controller
        // ---------------------------------- 
        thisAddress = config.getInetSocketAddress();
        failoverTaskRunner = new FailoverTaskRunner();
        serviceInterface = serviceLayer;
        leaseHolder = new FleaseHolder(REPLICATION_CELL, this);
        
        // ----------------------------------
        // initialize Flease
        // ----------------------------------
        File bDir = new File(config.getBabuDBConfig().getBaseDir());
        if (!bDir.exists()) bDir.mkdirs();

        fleaseParticipants = new LinkedList<InetSocketAddress>(config.getParticipants());       
        fleaseStage = new FleaseStage(config.getFleaseConfig(), 
                config.getBabuDBConfig().getBaseDir(), 
                new FleaseMessageSender(serviceLayer.getParticipantOverview(), 
                                        config.getInetSocketAddress()), 
                false, 
                new FleaseViewChangeListenerInterface() {
            
                    @Override
                    public void viewIdChangeEvent(ASCIIString cellId, int viewId) { 
                        throw new UnsupportedOperationException("Not supported yet.");
                    }
                    
                }, leaseHolder, null);
    }  
        
/*
 * overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.TopLayer#lockAll()
     */
    @Override
    public void lockAll() throws InterruptedException {
        proxyRequestCtrl.enableQueuing();
        userInterface.lock();
        replicationInterface.lock();
        serviceInterface.reset();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.TopLayer#unlockUser()
     */
    @Override
    public void unlockUser() {
        userInterface.unlock();
        proxyRequestCtrl.processQueue();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.TopLayer#unlockReplication()
     */
    @Override
    public void unlockReplication() {
        replicationInterface.unlock();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlLayerInterface#notifySuccessfulFailover(java.net.InetSocketAddress)
     */
    @Override
    public void notifyForSuccessfulFailover(InetSocketAddress master) {
        synchronized (master) {
            masterLock.set(master);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlLayerInterface#getLeaseHolder()
     */
    @Override
    public InetSocketAddress getLeaseHolder(int timeout) throws InterruptedException {
        return leaseHolder.getLeaseHolderAddress(timeout);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlLayerInterface#isItMe(
     *          java.net.InetSocketAddress)
     */
    @Override
    public boolean isItMe(InetSocketAddress address) {
        return thisAddress.equals(address);
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public void start() {        
        timeDriftDetector.start();
        failoverTaskRunner.start();
        fleaseStage.start();
        
        try {
            fleaseStage.waitForStartup();
        } catch (Exception e) {
            listener.crashPerformed(e);
        }
        
        joinFlease();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlLayerInterface#waitForInitialFailover()
     */
    @Override
    public void waitForInitialFailover() throws InterruptedException {
        synchronized (initialFailoverObserved) {
            if (!initialFailoverObserved.get()) {
                initialFailoverObserved.wait();
                
                if (!initialFailoverObserved.get()) {
                    throw new InterruptedException();
                }
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#shutdown()
     */
    @Override
    public void shutdown() {
        exitFlease();
        
        timeDriftDetector.shutdown();
        fleaseStage.shutdown();
        
        try {
            fleaseStage.waitForShutdown();
        } catch (Exception e) {
            listener.crashPerformed(e);
        }
        failoverTaskRunner.shutdown();
        
        synchronized (initialFailoverObserved) {
            initialFailoverObserved.notify();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#_setLifeCycleListener(org.xtreemfs.foundation.LifeCycleListener)
     */
    @Override
    public void _setLifeCycleListener(LifeCycleListener listener) {
        failoverTaskRunner.setLifeCycleListener(listener);
        timeDriftDetector.setLifeCycleListener(listener);
        fleaseStage.setLifeCycleListener(listener);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        exitFlease();
        
        failoverTaskRunner.shutdown();
        timeDriftDetector.shutdown();
        fleaseStage.shutdown();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.TimeDriftDetector.TimeDriftListener#
     *          driftDetected()
     */
    @Override
    public void driftDetected() {
        listener.crashPerformed(new Exception("Illegal time-drift " +
                "detected! The servers participating at the replication" +
                " are not synchronized anymore. Mutual exclusion cannot" +
                " be ensured. Replication is stopped immediately."));
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.FleaseMessageReceiver#receive(
     *          org.xtreemfs.foundation.flease.comm.FleaseMessage)
     */
    @Override
    public void receive(FleaseMessage message) {
        fleaseStage.receiveMessage(message);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.TopLayer#registerUserInterface(
     *          org.xtreemfs.babudb.replication.LockableService)
     */
    @Override
    public void registerUserInterface(LockableService service) {
        userInterface = service;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.TopLayer#registerReplicationInterface(
     *          org.xtreemfs.babudb.replication.LockableService)
     */
    @Override
    public void registerReplicationControl(LockableService service) {
        replicationInterface = service;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlLayerInterface#registerProxyRequestControl(org.xtreemfs.babudb.replication.transmission.RequestControl)
     */
    @Override
    public void registerProxyRequestControl(RequestControl control) {
        proxyRequestCtrl = control;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.FleaseEventListener#updateLeaseHolder(
     *          java.net.InetSocketAddress)
     */
    @Override
    public void updateLeaseHolder(InetSocketAddress newLeaseHolder) {
        failoverTaskRunner.queueFailoverRequest(newLeaseHolder);
    }
    
/*
 * private methods
 */

    /**
     * Method to participate at {@link Flease}.
     */
    private void joinFlease() {
        fleaseStage.openCell(REPLICATION_CELL, fleaseParticipants, false);
    }
    
    /**
     * Method to exclude this BabuDB instance from {@link Flease}.
     */
    private void exitFlease() {
        fleaseStage.closeCell(REPLICATION_CELL, true);
    }
    
    /**
     * This thread enables the replication to handle failover requests asynchronously to incoming 
     * Flease messages. It also ensures, that there will be only one failover at a time.
     * 
     * @author flangner
     * @since 02/21/2011
     */
    private final class FailoverTaskRunner extends LifeCycleThread {
        
        private final AtomicReference<InetSocketAddress> failoverRequest = 
            new AtomicReference<InetSocketAddress>(null);
        
        private boolean quit = true;
        
        private FailoverTaskRunner() {
            super("FailOverT@" + thisAddress.getPort());
        }
        
        /**
         * Enqueue a new failover request.
         * 
         * @param address - of the new replication master candidate.
         */
        void queueFailoverRequest(InetSocketAddress address) {
            Logging.logMessage(Logging.LEVEL_INFO, this, 
                    "Server %s is initiating failover with new master candidate %s.", 
                    thisAddress.toString(), address.toString());
            
            synchronized (failoverRequest) {
                if (failoverRequest.compareAndSet(null, address)) {
                    failoverRequest.notify();
                }
            }
        }
        
        /* (non-Javadoc)
         * @see java.lang.Thread#start()
         */
        @Override
        public synchronized void start() {
            super.start();
        }
        
        /* (non-Javadoc)
         * @see org.xtreemfs.foundation.LifeCycleThread#shutdown()
         */
        @Override
        public void shutdown() {
            quit = true;
            interrupt();
        }
        
        /* (non-Javadoc)
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            quit = false;
            notifyStarted();
            
            InetSocketAddress newLeaseHolder = null;
            try {
                while (!quit) {
                    
                    synchronized (failoverRequest) {
                        if (failoverRequest.get() == null) {
                            failoverRequest.wait();
                        }
                        
                        newLeaseHolder = failoverRequest.getAndSet(null);
                    }
                    
                    try {
                        
                        prepareFailover(newLeaseHolder);
                        
                        if (thisAddress.equals(newLeaseHolder)) {
                            becomeMaster();
                        } else {
                            becomeSlave(newLeaseHolder);
                        }
                        
                        synchronized (initialFailoverObserved) {
                            if (initialFailoverObserved.compareAndSet(false, true)) {
                                initialFailoverObserved.notify();
                            }
                        }
                    } catch (Exception e) {
                        if (!quit) {
                            Logging.logMessage(Logging.LEVEL_WARN, this, "Processing a failover " +
                            		"did not succeed, because: ", e.getMessage());
                            leaseHolder.reset();
                        }
                    }
                }
            } catch (InterruptedException e) {
                if (!quit) {
                    notifyCrashed(e);
                }
            }
            
            notifyStopped();
        }
        
        /**
         * Causes a solid state of BabuDB to prepare the failover. Therefore all local executions 
         * will be stopped. By checking the masterLock this method also ensures that BabuDB will not
         * be locked after unlock.
         * 
         * @param newLeaseholder
         * @throws InterruptedException
         */
        private void prepareFailover(InetSocketAddress newLeaseholder) throws InterruptedException {
            
            synchronized (masterLock) {
                InetSocketAddress mLock = masterLock.get();
                
                // we change the locally registered master
                if (mLock != null && !newLeaseholder.equals(masterLock.get())) {
                    try {
                        lockAll();
                    } finally {
                        unlockReplication();
                    }
                }
            }
        }
        
        /**
         * This server has to become the new master.
         * 
         * @throws Exception
         */
        private void becomeMaster() throws Exception {
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "Becoming the replication master.");
                  
            // synchronize with other servers 
            serviceInterface.synchronize(new SyncListener() {
                
                @Override
                public void synced(LSN lsn) {
                    
                    notifyForSuccessfulFailover(thisAddress);
                    unlockUser();
                    
                    Logging.logMessage(Logging.LEVEL_INFO, this, "Master failover succeeded.");
                }
                
                @Override
                public void failed(Exception ex) {
                    
                    Logging.logMessage(Logging.LEVEL_WARN, this, 
                            "Master failover did not succeed! Reseting the local lease and " +
                            "waiting for a new impulse from FLease. Reason: %s", ex.getMessage());
                    
                    leaseHolder.reset();
                }
            }, thisAddress.getPort());
            
            // if a new failover request arrives while synchronization is still waiting for a stable
            // state to be established (SyncListener), the listener will be marked as failed when
            // the new master address is set
        }
        
        /**
         * Another server has become the master and this one has to obey.
         * 
         * @param masterAddress
         * @throws InterruptedException 
         */
        private void becomeSlave(InetSocketAddress masterAddress) throws InterruptedException {
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "Becoming a slave for %s.", 
                    masterAddress.toString());
            
            // user requests may only be permitted on slaves that have been synchronized with the 
            // master, which is only possible after the master they obey internally has been changed 
            // by this method
        }
    }
}