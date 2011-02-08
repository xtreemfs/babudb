/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.control;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.TopLayer;
import org.xtreemfs.babudb.replication.control.TimeDriftDetector.TimeDriftListener;
import org.xtreemfs.babudb.replication.service.ServiceToControlInterface;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.MasterEpochHandlerInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.flease.proposer.FleaseListener;

/**
 * Contains the control logic for steering the replication process.
 * 
 * @author flangner
 * @since 02/24/2010
 */
public class ControlLayer extends TopLayer implements RoleChangeListener, 
    TimeDriftListener, FleaseMessageReceiver {
    
    /** designation of the flease-cell */
    private final static ASCIIString        REPLICATION_CELL = 
        new ASCIIString("replication");
    
    /** always access {@link Flease} from here */
    private final FleaseStage               fleaseStage;
    
    private final List<InetSocketAddress>   fleaseParticipants;
    
    /**
     * component to ensure {@link Flease}'s requirement of loosely synchronized
     * clocks
     */
    private final TimeDriftDetector         timeDriftDetector;
    
    /** controller for the replication mechanism */
    private final ReplicationController     replicationController;

    /** interface to the underlying layer */
    private final ServiceToControlInterface serviceInterface;
    
    /** the local address of this server to compare with the leaseHolders address */
    private final InetSocketAddress         id;
    
    private final FleaseHolder              leaseHolder;
    
    private final AtomicInteger             viewID;
    
    private final AtomicLong                sequenceNumber;
    
    public ControlLayer(ServiceToControlInterface service, 
            ReplicationConfig config) 
        throws IOException {
        
        this.viewID = new AtomicInteger(0);
        this.sequenceNumber = new AtomicLong(0L);
        this.serviceInterface = service;
        this.fleaseParticipants = new LinkedList<InetSocketAddress>(
                config.getParticipants());
        
        // ----------------------------------
        // initialize the time drift detector
        // ----------------------------------
        this.timeDriftDetector = new TimeDriftDetector(this, 
                service.getParticipantOverview().getConditionClients(), 
                config.getLocalTimeRenew());
        
        // ----------------------------------
        // initialize the replication 
        // controller
        // ---------------------------------- 
        this.id = FleaseHolder.getAddress(config.getFleaseConfig().getIdentity());
        this.leaseHolder = new FleaseHolder(REPLICATION_CELL);
        
        this.replicationController = 
            new ReplicationController(this.leaseHolder, this.serviceInterface, 
                    config.getAddress(), this);
        
        this.leaseHolder.registerListener(this.replicationController);
        
        // ----------------------------------
        // initialize Flease
        // ----------------------------------
        File bDir = new File(config.getBabuDBConfig().getBaseDir());
        if (!bDir.exists()) bDir.mkdirs();

        this.fleaseStage = new FleaseStage(config.getFleaseConfig(), 
                config.getBabuDBConfig().getBaseDir(), 
                new FleaseMessageSender(service.getParticipantOverview()), false, 
                new FleaseViewChangeListenerInterface() {
            
                    /* (non-Javadoc)
                     * @see org.xtreemfs.foundation.flease.
                     *          FleaseViewChangeListenerInterface#
                     * viewIdChangeEvent(
                     *          org.xtreemfs.foundation.buffer.ASCIIString, int)
                     */
                    @Override
                    public void viewIdChangeEvent(ASCIIString cellId, 
                            int viewId) {
                        
                        int oldViewId = viewID.getAndSet(viewId);
                        
                        // a master change has just occurred
                        if (oldViewId < viewId && !hasLease()) {
                            // TODO make the slave load from master
                        }
                    }
                }, this.leaseHolder, new MasterEpochHandlerInterface() {
                    
                    @Override
                    public void storeMasterEpoch(FleaseMessage request, 
                                                 Continuation callback) {
                        
                        synchronized (sequenceNumber) {
                            long epoch = request.getMasterEpochNumber();
                            
                            if (sequenceNumber.get() < epoch) {
                                sequenceNumber.set(epoch);
                                // TODO update (rebuild) last acknowledged
                            }
                        }
                        
                        callback.processingFinished();
                    }
                    
                    @Override
                    public void sendMasterEpoch(FleaseMessage response, 
                                                Continuation callback) {
                        // TODO Auto-generated method stub
                        
                        
                        callback.processingFinished();
                    }
                });
    }  
    
    /**
     * Method to participate at {@link Flease}.
     */
    void joinFlease() {
        this.fleaseStage.openCell(REPLICATION_CELL, this.fleaseParticipants, 
                true);
    }
    
    /**
     * Method to end lease ownership and transfer it to the newOwner.
     * 
     * @param newOwner
     * 
     * @return true, if the ownership has been transfered successfully, 
     *         false otherwise.
     * @throws InterruptedException 
     */
    boolean handOverLease(ASCIIString newOwner) throws InterruptedException {
        
        // this is result and lock to synchronize the request altogether
        final AtomicBoolean result = new AtomicBoolean();
        
        synchronized (result) {
            this.fleaseStage.handoverLease(REPLICATION_CELL, newOwner, 
                    new FleaseListener() {
                
                @Override
                public void proposalResult(ASCIIString cellId, 
                        ASCIIString leaseHolder, long leaseTimeout_ms,
                        long masterEpochNumber) {
                    
                    synchronized (result) {
                        result.set(true);
                        result.notify();
                    }
                }
                
                @Override
                public void proposalFailed(ASCIIString cellId, 
                                           Throwable cause) {
                    
                    synchronized (result) {
                        result.set(false);
                        result.notify();
                    }
                }
            });
            
            result.wait();
        }
        
        return result.get();
    }
    
    /**
     * Method to increment the viewId for this replication cell. This mechanism
     * is used to notify the slaves about a lease owner switch.
     * 
     * @return true, if the viewId has been changed successfully, 
     *         false otherwise.
     *         
     * @throws InterruptedException 
     */
    boolean incrementViewID() throws InterruptedException {
        
        // this is result and lock to synchronize the request altogether
        final AtomicBoolean result = new AtomicBoolean();
        
        synchronized (result) {
            this.fleaseStage.setViewId(REPLICATION_CELL, 
                    this.viewID.incrementAndGet(), new FleaseListener() {
                
                @Override
                public void proposalResult(ASCIIString cellId, 
                        ASCIIString leaseHolder, long leaseTimeout_ms,
                        long masterEpochNumber) {
                    
                    synchronized (result) {
                        result.set(true);
                        result.notify();
                    }
                }
                
                @Override
                public void proposalFailed(ASCIIString cellId, 
                        Throwable cause) {
                    
                    synchronized (result) {
                        
                        // reset the viewID on failure
                        viewID.decrementAndGet();
                        
                        result.set(false);
                        result.notify();
                    }
                }
            });
        
            result.wait();
        }
        
        return result.get();
    }
    
    /**
     * @return a list of servers participating, ordered by the latest LSN they
     *         have acknowledged.
     */
    List<InetSocketAddress> getClients() {
        List<InetSocketAddress> result = new ArrayList<InetSocketAddress>();
        for (ConditionClient c : 
            serviceInterface.getParticipantOverview().getConditionClients()) {
            result.add(c.getDefaultServerAddress());
        }
        return result;
    }
    
    /**
     * Method to exclude this BabuDB instance from {@link Flease}.
     */
    void exitFlease() {
        this.fleaseStage.closeCell(REPLICATION_CELL);
    }
    
    /**
     * @return a controller for the {@link TimeDriftDetector}.
     */
    TimeDriftDetectorControl getTimeDriftDetectorControl() {
        return this.timeDriftDetector;
    }
    
    boolean amIMaster() {
        return amIMaster(leaseHolder.getLeaseHolderAddress());
    }
    
/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlToBabuDBInterface#replicate(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer)  {  
        String errorMsg = null;
        try {
            if (replicationController.hasLease()) {              
                if (!replicationController.isSuspended()) {
                    return serviceInterface.replicate(le, buffer);
                } else {
                    errorMsg = "Replication is suspended at the moment. " +
                               "Try again later.";
                }
            } else {
                errorMsg = "This BabuDB is not running in master-mode! The " +
                                "operation could not be replicated.";
            }
        } catch (InterruptedException e) {
            errorMsg = "Checking the lease of this server has been interrupted." +
                        " It could be still performing a failover.";
        }   
        assert (errorMsg != null);
        return new ReplicateResponse(le, 
                new BabuDBException(ErrorCode.REPLICATION_FAILURE, errorMsg));
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlToBabuDBInterface#subscribeListener(org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse)
     */
    @Override
    public void subscribeListener(ReplicateResponse rp) {
        synchronized (this.replicationController) {
            if (this.replicationController.isSuspended() && !rp.hasFailed()) {
                rp.failed();
            } else {
                this.serviceInterface.subscribeListener(rp);
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlToBabuDBInterface#getLeaseHolder()
     */
    @Override
    public InetSocketAddress getLeaseHolder() {
        return leaseHolder.getLeaseHolderAddress();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlToBabuDBInterface#amIMaster(
     *          java.net.InetSocketAddress)
     */
    @Override
    public boolean amIMaster(InetSocketAddress master) {
        if (master != null) {
            return this.id.equals(master);
        }
        
        return false;
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public void start() {        
        this.replicationController.start();
        this.timeDriftDetector.start();
        this.fleaseStage.start();
        
        try {
            this.replicationController.waitForStartup();
            this.fleaseStage.waitForStartup();
        } catch (Exception e) {
            this.listener.crashPerformed(e);
        }
        
        joinFlease();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#shutdown()
     */
    @Override
    public void shutdown() {
        this.replicationController.shutdown();
        this.timeDriftDetector.shutdown();
        this.fleaseStage.shutdown();
        
        try {
            this.fleaseStage.waitForShutdown();
            this.replicationController.waitForShutdown();
        } catch (Exception e) {
            this.listener.crashPerformed(e);
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#_setLifeCycleListener(org.xtreemfs.foundation.LifeCycleListener)
     */
    @Override
    public void _setLifeCycleListener(LifeCycleListener listener) {
        this.timeDriftDetector.setLifeCycleListener(listener);
        this.fleaseStage.setLifeCycleListener(listener);
        this.replicationController.setLifeCycleListener(listener);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        this.timeDriftDetector.shutdown();
        this.fleaseStage.shutdown();
        this.replicationController.shutdown();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.TimeDriftDetector.TimeDriftListener#driftDetected()
     */
    @Override
    public void driftDetected() {
        this.listener.crashPerformed(new Exception("Illegal time-drift " +
                "detected! The servers participating at the replication" +
                " are not synchronized anymore. Mutual exclusion cannot" +
                " be ensured. Replication is stopped immediately."));
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.RoleChangeListener#suspend()
     */
    @Override
    public void suspend() {
        this.replicationController.notifyForSuspension();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlToBabuDBInterface#hasLease()
     */
    @Override
    public boolean hasLease() {
        return !this.replicationController.isSuspended() && 
                this.id.equals(this.leaseHolder.getLeaseHolderAddress());
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.FleaseMessageReceiver#receive(
     *          org.xtreemfs.foundation.flease.comm.FleaseMessage)
     */
    @Override
    public void receive(FleaseMessage message) {
        this.fleaseStage.receiveMessage(message);
    }
}