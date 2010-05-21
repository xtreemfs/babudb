/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.TopLayer;
import org.xtreemfs.babudb.replication.control.TimeDriftDetector.TimeDriftListener;
import org.xtreemfs.babudb.replication.service.ServiceToControlInterface;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;

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
    
    private final FleaseHolder              leaseHolder;
    
    public ControlLayer(ServiceToControlInterface service, 
            ReplicationConfig config) 
        throws IOException {
        
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
        this.leaseHolder = new FleaseHolder(REPLICATION_CELL, 
                config.getFleaseConfig().getIdentity());
        
        this.replicationController = 
            new ReplicationController(this.leaseHolder, this.serviceInterface, 
                    config.getAddress(), this);
        
        this.leaseHolder.registerListener(this.replicationController);
        
        // ----------------------------------
        // initialize Flease
        // ----------------------------------
        File bDir = new File(config.getBaseDir());
        if (!bDir.exists()) bDir.mkdirs();

        this.fleaseStage = new FleaseStage(config.getFleaseConfig(), 
                config.getBaseDir(), 
                new FleaseMessageSender(service.getParticipantOverview()), true, 
                new FleaseViewChangeListenerInterface() {
                    @Override
                    public void viewIdChangeEvent(ASCIIString cellId, 
                            int viewId) {
                        /* ignored */
                    }
                }, this.leaseHolder);
    }  
    
    /**
     * Method to participate at the {@link Flease} mechanism.
     */
    void joinFlease() {
        this.fleaseStage.openCell(REPLICATION_CELL, this.fleaseParticipants);
    }
    
    /**
     * Method to exclude from the {@link Flease} mechanism.
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
    
/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.control.ControlToBabuDBInterface#replicate(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer)  {  
        String errorMsg = null;
        try {
            if (this.replicationController.hasLease()) {              
                if (!this.replicationController.isSuspended()) {
                    return this.serviceInterface.replicate(le, buffer);
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
    public InetAddress getLeaseHolder() {
        InetSocketAddress address = this.leaseHolder.getLeaseHolderAddress();
        
        return (address != null) ? address.getAddress() : null;
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
                this.leaseHolder.amIOwner();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.FleaseMessageReceiver#receive(org.xtreemfs.foundation.flease.comm.FleaseMessage)
     */
    @Override
    public void receive(FleaseMessage message) {
        this.fleaseStage.receiveMessage(message);
    }
}