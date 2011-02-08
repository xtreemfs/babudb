/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.control.RoleChangeListener;
import org.xtreemfs.babudb.replication.proxy.RPCRequestHandler;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.transmission.TransmissionToServiceInterface;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Contains the service logic for all necessary operations used by the 
 * replication. 
 *
 * @author flangner
 * @since 04/12/2010
 */
public class ServiceLayer extends Layer implements  ServiceToControlInterface, SlaveView {
    
    /** states of all the other participants */
    private final ParticipantsStates             participantsStates;
    
    /** continuously sends heartbeat-messages to all participants */
    private final HeartbeatThread                heartbeatThread;
    
    /** thread to process replication requests from a master server */
    private final ReplicationStage               replicationStage;
    
    /** the last acknowledged LSN of the last view */
    private final AtomicReference<LSN>           lastOnView = 
        new AtomicReference<LSN>();
    
    /** interface for operation performed on BabuDB */
    private final BabuDBInterface                babuDBInterface;
    
    /** listener to notify about changes of this server's role */
    private volatile RoleChangeListener          roleChangeListener = null;
    
    /** interface to the underlying layer */
    private final TransmissionToServiceInterface transmissionInterface;
        
    private final int maxChunkSize;
    
    /**
     * 
     * @param config
     * @param underlyingLayer
     * @param clientFactory
     * @param latest
     * @throws IOException if {@link Flease} could not be initialized.
     */
    public ServiceLayer(ReplicationConfig config, BabuDBInterface babuDB,
            TransmissionToServiceInterface transLayer) throws IOException {
        
        this.maxChunkSize = config.getChunkSize();
        this.transmissionInterface = transLayer;
        this.babuDBInterface = babuDB;
        
        // ----------------------------------
        // initialize the participants states
        // ----------------------------------
        int syncN = ((config.getSyncN() > 0) ? config.getSyncN()-1 : 
                                               config.getSyncN());
        this.participantsStates = new ParticipantsStates(syncN, 
                config.getParticipants(), transLayer);
        
        // ----------------------------------
        // initialize the heartbeat
        // ----------------------------------
        this.heartbeatThread = new HeartbeatThread(participantsStates);
        
        // ----------------------------------
        // initialize replication stage
        // ----------------------------------
        this.replicationStage = new ReplicationStage(
                config.getBabuDBConfig().getMaxQueueLength(), 
                this.heartbeatThread, this, transLayer.getFileIOInterface(), 
                this.babuDBInterface, this.lastOnView, maxChunkSize);
        
        // ----------------------------------
        // initialize request logic for 
        // handling BabuDB remote calls
        // ----------------------------------
        this.transmissionInterface.addRequestHandler(
                new RPCRequestHandler(participantsStates, babuDBInterface));
    }
    
/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#replicate(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.foundation.buffer.ReusableBuffer)
     */
    @Override
    @SuppressWarnings("unchecked")
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer payload) {
        List<SlaveClient> slaves;
        try {
            slaves = this.participantsStates.getAvailableParticipants();
        } catch (Exception e) {
            return new ReplicateResponse(le, e);
        }  
        final ReplicateResponse result = new ReplicateResponse(le,
                slaves.size() - this.participantsStates.getSyncN());
        
        // make the replicate call at the clients
        if (slaves.size() == 0) { 
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                    "There are no slaves available anymore! " +
                    "BabuDB runs if it would be in non-replicated mode.");
        } else {  
            for (final SlaveClient slave : slaves) {
                ((ClientResponseFuture<Object>) slave.replicate(le.getLSN(), 
                        payload.createViewBuffer())).registerListener(
                                new ClientResponseAvailableListener<Object>() {
                
                    @Override
                    public void responseAvailable(Object r) {
                        // evaluate the response
                        participantsStates.requestFinished(slave);
                    }

                    @Override
                    public void requestFailed(Exception e) {
                        participantsStates.markAsDead(slave);
                        result.decrementPermittedFailures();
                        
                        Logging.logMessage(Logging.LEVEL_INFO, this, 
                                "'%s' was marked as dead, because %s", 
                                slave.getDefaultServerAddress().toString(), 
                                e.getMessage());
                        if (e.getMessage() == null)
                            Logging.logError(Logging.LEVEL_DEBUG, this, e);
                    }
                });
            }
        }    
        return result;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#synchronize()
     */
    @Override
    public void synchronize() throws 
        BabuDBException, InterruptedException, IOException {
        
        List<ConditionClient> slaves = 
            this.participantsStates.getConditionClients();
 
        // get time-stamp to decide when to start the replication
        long now = TimeSync.getLocalSystemTime();
    
        // get the most up-to-date slave
        LSN latest = null;
        Map<ClientInterface ,LSN> states = null;
    
        if (slaves.size() > 0) {
            states = getStates(slaves);
        
            // handover the lease, if not enough slaves are available 
            // to assure consistency
            if ((states.size()+1) < this.participantsStates.getSyncN()) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                        "Not enough slaves available to synchronize with!");
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
        LSN localState = babuDBInterface.getState();
        if (latest != null && latest.compareTo(localState) > 0) {
            for (Entry<ClientInterface, LSN> entry : states.entrySet()) {
                if (entry.getValue().equals(latest)) {                 
                    // setup a slave dispatcher to synchronize the 
                    // BabuDB with a replicated participant         
                    Logging.logMessage(Logging.LEVEL_INFO, this, 
                            "Starting synchronization from '%s' to '%s'.", 
                            localState.toString(), latest.toString());
                
                    BabuDBRequestResultImpl<Boolean> ready = 
                        new BabuDBRequestResultImpl<Boolean>();
                    this.replicationStage.manualLoad(ready, localState, latest);
                    ready.get();
                
                    assert(latest.equals(babuDBInterface.getState())) : 
                        "Synchronization failed: (expected=" + 
                        latest.toString() + ") != (acknowledged=" + 
                        babuDBInterface.getState() + ")";
                    break;
                }
            }
        }
    
        // always take a checkpoint on master-failover (incl. viewId inc.)
        this.lastOnView.set(this.babuDBInterface.getState());
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "taking a checkpoint");
        // TODO maybe there are still some inserts on the DiskLogger-queue,
        // which might increment the lastOnView LSN.
        this.babuDBInterface.checkpoint();
    
        // wait for the slaves to recognize the master-change, 
        // before setting up the masterDispatcher (asynchronous)
        long difference = TimeSync.getLocalSystemTime() - now;
        long threshold = ReplicationConfig.LEASE_TIMEOUT / 2;
        Thread.sleep((difference < threshold ? threshold-difference : 0 ));
        
        // reset the new master
        latest = this.babuDBInterface.getState();
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "Running in master" +
                "-mode (%s)", latest.toString());
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Coinable#coin(java.lang.Object, java.lang.Object)
     */
    @Override
    public void coin(RoleChangeListener listener, 
            FleaseMessageReceiver receiver) {
        assert (receiver != null);
        assert (listener != null);
        
        synchronized (this) {
            if (this.roleChangeListener == null) {
                this.roleChangeListener = listener;  
                this.replicationStage.setRoleChangeListener(listener);
                
                // ----------------------------------
                // coin the dispatcher of the 
                // underlying layer
                // ----------------------------------
                this.transmissionInterface.addRequestHandler(
                        new ReplicationRequestHandler(participantsStates, 
                                receiver, babuDBInterface, replicationStage, 
                                lastOnView, maxChunkSize, 
                                transmissionInterface.getFileIOInterface()));
            }
        }
    }
  
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.SlaveView#getSynchronizationPartner(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public MasterClient getSynchronizationPartner(LSN progressAtLeast) {
        MasterClient master = this.participantsStates.getMaster();
        
        List<ConditionClient> servers = 
            this.participantsStates.getConditionClients();

        servers.remove(master);
        
        if (servers.size() > 0) {
            Map<ClientInterface, LSN> states = getStates(servers);
            
            for (Entry<ClientInterface, LSN> e : states.entrySet()) {
                if (e.getValue().compareTo(progressAtLeast) >= 0)
                    return (MasterClient) e.getKey();
            }
        }
        
        return master;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#getParticipantOverview()
     */
    @Override
    public ParticipantsOverview getParticipantOverview() {
        return this.participantsStates;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#changeMaster(java.net.InetAddress)
     */
    @Override
    public void changeMaster(InetAddress address) {
        replicationStage.lastInserted = babuDBInterface.getState();
        // TODO maybe there are still some inserts on the DiskLogger-queue,
        // which might increment the lastOnView LSN.
        participantsStates.setMaster(address);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#_setLifeCycleListener(org.xtreemfs.foundation.LifeCycleListener)
     */
    @Override
    public void _setLifeCycleListener(LifeCycleListener listener) {
        this.heartbeatThread.setLifeCycleListener(listener);
        this.replicationStage.setLifeCycleListener(listener);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#start()
     */
    @Override
    public void start() {
        LSN latest = babuDBInterface.getState();
        
        // the sequence number of the initial LSN before incrementing the 
        // viewID must not be 0 
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Setting last on view " +
           "LSN to '%s', initial was '%s'.", (latest.getSequenceNo() == 0L) ? 
                        new LSN(0,0L) : latest,latest);
        
        this.lastOnView.set((latest.getSequenceNo() == 0L) ? 
                new LSN(0,0L) : latest);
                
        try {
            if (this.roleChangeListener == null)
                throw new Exception("The service layer has not been coined " +
                		"yet!");
            
            this.heartbeatThread.start(latest);
            this.replicationStage.lastInserted = latest;
            this.replicationStage.start();
            this.heartbeatThread.waitForStartup();
            this.replicationStage.waitForStartup();
        } catch (Exception e) {
            this.listener.crashPerformed(e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#asyncShutdown()
     */
    @Override
    public void asyncShutdown() {
        this.heartbeatThread.shutdown();
        this.replicationStage.shutdown();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#shutdown()
     */
    @Override
    public void shutdown() {
        this.heartbeatThread.shutdown();
        this.replicationStage.shutdown();
        
        try {
            this.heartbeatThread.waitForShutdown();
            this.replicationStage.waitForShutdown();
        } catch (Exception e) {
            this.listener.crashPerformed(e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#subscribeListener(org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse)
     */
    @Override
    public void subscribeListener(ReplicateResponse rp) {
        this.participantsStates.subscribeListener(rp);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#reset()
     */
    @Override
    public void reset() {
        this.participantsStates.clearListeners();
    }
    
/*
 * private methods
 */
    
    /**
     * <p>
     * Performs a network broadcast to get the latest LSN from every available 
     * DB.
     * </p>
     * 
     * @param clients
     * @return the LSNs of the latest written LogEntries for the <code>babuDBs
     *         </code> that were available.
     */
    private Map<ClientInterface, LSN> getStates(List<ConditionClient> clients) {
        int numRqs = clients.size();
        Map<ClientInterface, LSN> result = 
            new HashMap<ClientInterface, LSN>();
        
        @SuppressWarnings("unchecked")
        ClientResponseFuture<LSN>[] rps = new ClientResponseFuture[numRqs];
        
        // send the requests
        for (int i=0; i<numRqs; i++) {
            rps[i] = clients.get(i).state();
        }
        
        // get the responses
        for (int i = 0; i < numRqs; i++) {
            try{
                LSN val = rps[i].get();
                result.put(clients.get(i), val);
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_INFO, this, 
                        "Could not receive state of '%s', because: %s.", 
                        clients.get(i), e.getMessage());
                if (e.getMessage() == null)
                    Logging.logError(Logging.LEVEL_INFO, this, e);
            } 
        }
        
        return result;
    }
}