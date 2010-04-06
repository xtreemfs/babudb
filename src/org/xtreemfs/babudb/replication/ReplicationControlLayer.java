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
package org.xtreemfs.babudb.replication;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.MasterRequestDispatcher;
import org.xtreemfs.babudb.replication.ReplicateResponse;
import org.xtreemfs.babudb.replication.RequestDispatcher;
import org.xtreemfs.babudb.replication.SlaveRequestDispatcher;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.babudb.replication.TimeDriftDetector.TimeDriftListener;
import org.xtreemfs.babudb.replication.stages.logic.SharedLogic;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStage;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.FleaseViewChangeListenerInterface;
import org.xtreemfs.foundation.flease.proposer.FleaseException;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>
 * Automatic failover mechanism.
 * 
 * Continuously updates the current lease holder via {@link Flease}.
 * Changes the state of the replication mechanism.
 * </p>
 * 
 * @author flangner
 * @since 02/24/2010
 */
public class ReplicationControlLayer extends LifeCycleThread implements 
    LifeCycleListener, FleaseStatusListener, TimeDriftListener {
    
    /** designation of the flease-cell */
    private final static ASCIIString        REPLICATION_CELL = 
        new ASCIIString("replication");
    
    final ReplicationConfig                 configuration;
    
    volatile boolean                        quit = true;
    
    /** always access {@link Flease} from here */
    final FleaseStage                       fleaseStage;
    
    /** the currently valid lease */
    private final AtomicReference<Flease>   flease = 
        new AtomicReference<Flease>(new Flease(REPLICATION_CELL, null, 0));
    
    private RequestDispatcher               dispatcher;
    
    /** necessary to support mutual exclusion on failover */
    private final AtomicBoolean             failoverInProgress = 
        new AtomicBoolean(true);
    private Flease                          failoverLease = flease.get();
    
    /** necessary to support mutual exclusion on handover */
    private final AtomicBoolean             handoverInProgress = 
        new AtomicBoolean(false);
    
    public ReplicationControlLayer(BabuDB dbs, LSN initial) throws Exception {
        super("ReplicationController");
        this.configuration = (ReplicationConfig) dbs.getConfig();
        
        // -------------------------------
        // initialize the dispatcher
        // -------------------------------
        this.dispatcher = new MasterRequestDispatcher(dbs, initial, this);
        
        // -------------------------------
        // initialize Flease
        // -------------------------------
        File bDir = new File(configuration.getBaseDir());
        if (!bDir.exists()) bDir.mkdirs();

        this.fleaseStage = new FleaseStage(configuration.getFleaseConfig(), 
                configuration.getBaseDir(), new FleaseMessageSender(
                        dispatcher.rpcClient, 
                        configuration.getInetSocketAddress()), true, 
                new FleaseViewChangeListenerInterface() {
                    @Override
                    public void viewIdChangeEvent(ASCIIString cellId, int viewId) {
                        /* ignored */
                    }
                }, this);
        this.fleaseStage.setLifeCycleListener(this);
    }
    
/*
 * Replication Manager-methods.
 */
    
    public ReplicationControlLayer(BabuDB dbs, LSN initial, 
            FleaseStage fleaseStage) throws Exception {
        super("ReplicationController");
        this.configuration = (ReplicationConfig) dbs.getConfig();
        
        // -------------------------------
        // initialize the dispatcher
        // -------------------------------
        this.dispatcher = new MasterRequestDispatcher(dbs, initial, this);
        
        // -------------------------------
        // initialize flease
        // -------------------------------
        this.fleaseStage = fleaseStage;
        this.fleaseStage.setLifeCycleListener(this);
    }
    
    /**
     * Performs a valid replicate-operation on all available slaves.
     * 
     * @param le
     * @param buffer
     * 
     * @return the {@link ReplicateResponse} future.
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer)  {   
        try {
            if (hasLease()) {
                return dispatcher.replicate(le, buffer);
            } else {
                return new ReplicateResponse(le, new BabuDBException(ErrorCode.
                        REPLICATION_FAILURE, "This BabuDB is not running in " +
                        "master-mode! The operation could not be replicated."));
            }
        } catch (InterruptedException e) {
            return new ReplicateResponse(le, new BabuDBException(ErrorCode.
                    REPLICATION_FAILURE, "Checking the lease of this server " +
                    "has been interrupted. It could be still performing a " +
                    "failover."));
        }   
    }
    
    /**
     * <p>
     * Registers the listener for a replicate call.
     * </p>
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse rp) {
        this.dispatcher.subscribeListener(rp);
    }
        
    /**
     * Gives the lease to another database server.
     * Only one handover is running at a time.
     * Failover will be stopped.
     * 
     * @param reason - may be null.
     */
    public void handoverLease(String reason) {
        // part one: acquire the lock
        synchronized (flease) {
            
            // check if this server is currently the leaseholder
            if (amIOwner(flease.get())){
                // set the handover in progress
                handoverInProgress.set(true);
                
                // interrupt running failover and suspend it
                synchronized (failoverInProgress) {
                    failoverInProgress.set(false);
                    this.interrupt();   
                }
            } else return;
        }

        // part two: process the handover
        synchronized(handoverInProgress) {
            // check if this server should perform a handover
            if (handoverInProgress.get()) {
                Logging.logMessage(Logging.LEVEL_INFO, this, "The lease is handed" +
                        " over to '%s' because '%s'", "anyone", reason);
                    
                // exclude from Flease
                fleaseStage.closeCell(REPLICATION_CELL);
                
                // suspend the replication on this server
                this.dispatcher.suspend();
                
                // wait for a lease-timeout to be completed
                long diff = flease.get().getLeaseTimeout_ms() - TimeSync.getGlobalTime();
                if (diff > 0) {
                    try {
                        handoverInProgress.wait(diff);
                    } catch (InterruptedException e) { /* I don't care */ }
                }
                
                // join Flease again
                fleaseStage.openCell(REPLICATION_CELL, 
                        new LinkedList<InetSocketAddress>(
                                configuration.getParticipants()));
                
                handoverInProgress.set(false);
            }
        }
        
        // choose a new master randomly
        /* TODO reactivate
        InetSocketAddress newMaster = configuration.getParticipants().iterator().next();
        ASCIIString newMasterId = new ASCIIString(getIdentity(newMaster));
        
        fleaseStage.handoverLease(REPLICATION_CELL, newMasterId, new FleaseListener() {
            
            @Override
            public void proposalResult(ASCIIString cellId, ASCIIString leaseHolder, long leaseTimeoutMs) {
                // I don't care 
            }
            
            @Override
            public void proposalFailed(ASCIIString cellId, Throwable cause) {
                // I don't care 
            }
        });
        
        newMaster.toString()
        */
    } 
    
    /**
     * Delays the request if there is a failover in progress.
     * 
     * @return true, if this server has the lease, false otherwise.
     */
    public boolean hasLease() throws InterruptedException{
        boolean result;
        synchronized (failoverInProgress) {
            while ((result = (!handoverInProgress.get() && 
                    amIOwner(flease.get()))) && failoverInProgress.get()) {
                
                    failoverInProgress.wait();
            }
            return result;
        } 
    }
    
    /**
     * @return the address of the current lease holder, or null if the lease is 
     *         not available at the moment.
     */
    public InetSocketAddress getLeaseHolder() {
        Flease lease = flease.get();
        
        InetSocketAddress holder; 
        if (!lease.isValid()) holder = null;
        else holder = getAddress(lease.getLeaseHolder());
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "getHolder=%s (%s) @%d", 
                (holder != null) ? holder.toString() : null, 
                (lease != null) ? lease.toString() : null,
                TimeSync.getGlobalTime());

        return holder;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#leaseFailed(org.xtreemfs.foundation.buffer.ASCIIString, org.xtreemfs.foundation.flease.proposer.FleaseException)
     */
    @Override
    public void leaseFailed(ASCIIString cellId, FleaseException error) {
        Logging.logMessage(Logging.LEVEL_WARN, this, "Flease was not" +
                " able to become the current lease holder because:" +
                " %s ", error.getMessage());
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#statusChanged(org.xtreemfs.foundation.buffer.ASCIIString, org.xtreemfs.foundation.flease.Flease)
     */
    @Override
    public void statusChanged(ASCIIString cellId, Flease lease) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "received '%s' at" +
                " time %d", lease.toString(), TimeSync.getGlobalTime());
        
        // outdated leases or broken leases will be ignored
        if (lease.isValid()) {
            synchronized (flease) {
                // check if the lease holder changed
                if (!lease.getLeaseHolder().equals(
                        flease.get().getLeaseHolder())) {
                    
                    // notify the handover if this server loses ownership of the 
                    // lease
                    if (handoverInProgress.get() && !amIOwner(lease)) {
                        synchronized (handoverInProgress) {
                            if (handoverInProgress.compareAndSet(true, false)) {
                                handoverInProgress.notify();
                            }
                        }
                    }
                    
                    // notify failover about the change
                    synchronized (failoverInProgress) {
                        failoverLease = lease;
                        
                        if (failoverInProgress.getAndSet(true)) {
                            this.interrupt();
                        } else {
                            failoverInProgress.notifyAll();
                        }
                    }
                }
                flease.set(lease);
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable exc) {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "crash: "+exc.getMessage()
                +" ... terminating BabuDB!");
        handoverLease(exc);
        quit = true;
        throw new RuntimeException(exc);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "terminated successfully.");
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "started successfully.");
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.TimeDriftDetector.TimeDriftListener#driftDetected()
     */
    @Override
    public void driftDetected() {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "Illegal time-drift " +
                        "detected! The servers participating at the replication" +
                        " are not synchronized anymore. Mutual exclusion cannot" +
                        " be ensured. Replication is stopped immediately.");
        
        try {
            shutdown();
        } catch (Exception e) {
            notifyCrashed(e);
        }
    }
    
    /**
     * @param address
     * @return the string representation of the given address.
     */
    public static String getIdentity (InetSocketAddress address) {
        String host = address.getAddress().getHostAddress();
        assert (host != null) : "Address was not resolved before!";
        return host + ":" + address.getPort();
    }
    
    /**
     * @param identity
     * @return the address used to create the given identity.
     */
    public static InetSocketAddress getAddress (ASCIIString identity) {
        String[] adr = identity.toString().split(":");
        assert(adr.length == 2);
        return new InetSocketAddress(adr[0], Integer.parseInt(adr[1]));
    }
    
    /**
     * <p>
     * Performs a network broadcast to get the latest LSN from every available DB.
     * </p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for the <code>babuDBs
     *         </code> that were available.
     */
    Map<InetSocketAddress, LSN> getStates(List<InetSocketAddress> babuDBs) {
        assert (dispatcher != null);
        return SharedLogic.getStates(babuDBs, dispatcher.rpcClient,
                configuration.getInetSocketAddress());
    }
    
    /**
     * Handover initialized through an exception.
     * 
     * @param exc
     */
    void handoverLease(Throwable exc) {
        handoverLease("ERROR: "+exc.getMessage());
    }
    
    /**
     * Reports the last-on-view LSN to the replication-mechanism, 
     * after taking a checkpoint.
     * 
     * @param lsn
     */
    void updateLastOnView(LSN lsn) {
        dispatcher.lastOnView.set(lsn);
    }
    
/*
 * Thread to change the dispatcher's state
 */
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public void start() {        
        // start the flease stage
        try {
            this.fleaseStage.start();
            this.fleaseStage.waitForStartup();
        } catch (Exception e) {
            crashPerformed(e);
        }
        
        quit = false;
        super.start();
        
        // start the dispatcher
        this.dispatcher.start();
        
        // initially get the leaseHolder
        this.fleaseStage.openCell(REPLICATION_CELL, 
                new LinkedList<InetSocketAddress>(
                        configuration.getParticipants()));
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        ASCIIString id = fleaseStage.getIdentity();
        ASCIIString newMaster;
        notifyStarted();
        
        while(!quit) {
            synchronized (failoverInProgress) {
                // wait for a failover request
                while (!failoverInProgress.get()) {
                    try {
                        failoverInProgress.wait();
                    } catch (InterruptedException e) {
                        if (quit) break;
                    }
                }
                if (quit) break;
                
                assert (failoverLease != null);
                newMaster = failoverLease.getLeaseHolder();
                failoverLease = null;
            }
                
            // process the failover request
            this.dispatcher.suspend();
            if (id.equals(newMaster)) {
                try {
                    // try to become a master; leads to a handover on failure
                    becomeMaster(newMaster, dispatcher.getState());
                } catch (InterruptedException ie) {
                    /* ignored */
                } catch (Exception e) {
                    Logging.logMessage(Logging.LEVEL_ERROR, this, 
                            "failover did not work, because: %s ", 
                            e.getMessage());
                    Logging.logError(Logging.LEVEL_INFO, this, e);
                    handoverLease("failover did not work, because: "+ 
                            e.getMessage());
                }
            } else if (newMaster != null){
                // become a slave
                becomeSlave(newMaster);
            } 
            
            // failover finished
            synchronized (failoverInProgress) {
                if (failoverInProgress.get() && failoverLease == null ) {
                    failoverInProgress.set(false);
                    failoverInProgress.notifyAll();
                }
            }
        }
       
        notifyStopped();
    }
    
    /**
     * This server has become the new master.
     * @param masterId
     * @param state
     * @throws InterruptedException 
     * @throws BabuDBException 
     * @throws IOException 
     */
    private void becomeMaster(ASCIIString masterId, DispatcherState state) 
        throws InterruptedException, BabuDBException, IOException {
        
        Logging.logMessage(Logging.LEVEL_INFO, this, 
                "Becoming the replication master.");
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>(
                    configuration.getParticipants());
     
        // get time-stamp to decide when to start the replication
        long now = TimeSync.getLocalSystemTime();
        
        // get the most up-to-date slave
        LSN latest = null;
        Map<InetSocketAddress,LSN> states = null;
        
        if (slaves.size() > 0) {
            states = SharedLogic.getStates(slaves, dispatcher.rpcClient,
                    configuration.getInetSocketAddress());
            
            // handover the lease, if not enough slaves are available 
            // to assure consistency
            if ((states.size()+1) < configuration.getSyncN()) {
                Logging.logMessage(Logging.LEVEL_INFO, this, "Not enough " +
                                "slaves available to synchronize with!");
                handoverLease("Not enough slaves available to synchronize with!");
                return;
            }
            
            if (states.size() > 0) {
                // if one of them is more up to date, then synchronize 
                // with it
                List<LSN> values = new LinkedList<LSN>(states.values());
                Collections.sort(values, Collections.reverseOrder());
                latest = values.get(0);
            }
        }
        
        // synchronize with the most up-to-date slave, if necessary
        boolean snapshot = true;
        
        if (latest != null && latest.compareTo(state.latest) > 0) {
            for (Entry<InetSocketAddress,LSN> entry : states.entrySet()) 
            {
                if (entry.getValue().equals(latest)) {
                    Logging.logMessage(Logging.LEVEL_INFO, this, 
                            "synchronize with '%s'", entry.getKey()
                            .toString());  
                    
                    // setup a slave dispatcher to synchronize the 
                    // BabuDB with a replicated participant
                    SlaveRequestDispatcher lDisp = null;
                    try {
                        lDisp = new SlaveRequestDispatcher(this.dispatcher, 
                                entry.getKey(), this);
                        this.dispatcher = lDisp;
                    
                        snapshot = lDisp.synchronize(latest);
                    } finally {
                        if (lDisp != null) lDisp.suspend();
                    }
                    break;
                }
            }
        }
        
        // make a snapshot
        if (snapshot && (latest.getSequenceNo() > 0L)) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "taking a new checkpoint");
            CheckpointerImpl cp = (CheckpointerImpl) dispatcher.dbs.getCheckpointer();
            cp.checkpoint(true);
        // switch the log-file only if there was have been replicated operations
        // since the last log-file-switch
        } else if (latest.getSequenceNo() > 0L){
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "switching the logfile only");
            DiskLogger logger = dispatcher.dbs.getLogger();
            try {
                logger.lockLogger();
                logger.switchLogFile(true);
            } finally {
                logger.unlockLogger();
            }
        }
        
        // wait for the slaves to recognize the master-change, 
        // before setting up the masterDispatcher (asynchronous)
        long difference = TimeSync.getLocalSystemTime() - now;
        long threshold = ReplicationConfig.LEASE_TIMEOUT / 2;
        Thread.sleep((difference < threshold ? threshold-difference : 0 ));
        
        // reset the new master
        latest = dispatcher.getState().latest;
        
        this.dispatcher = new MasterRequestDispatcher(dispatcher, getAddress(masterId));
        Logging.logMessage(Logging.LEVEL_INFO, dispatcher, "Running in master" +
        	"-mode (%s)", latest.toString());
    }
    
    /**
     * Another server has become the master and this one has to obey.
     * @param masterId
     */
    private void becomeSlave(final ASCIIString masterId) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "Becoming a slave for %s.", 
                masterId.toString());
        this.dispatcher = new SlaveRequestDispatcher(this.dispatcher, 
                getAddress(masterId), this);
    }
    
    /**
     * Determines if this server is owner of the given lease or not.
     * 
     * @param lease
     * @return true if this server is owner of the lease, false otherwise.
     */
    private boolean amIOwner(Flease lease) {
        return lease.isValid() && 
               fleaseStage.getIdentity().equals(lease.getLeaseHolder());
    }
    
    /**
     * Terminate the replication threads.
     * @throws Exception if shutdown of the flease communicator failed.
     */
    public void shutdown() throws Exception{
        quit = true;
        synchronized (failoverInProgress) {
            failoverInProgress.set(false);
            this.interrupt();
            failoverInProgress.notifyAll();
        }
        this.dispatcher.shutdown();
        this.fleaseStage.shutdown();
        this.fleaseStage.waitForShutdown();
    }
}