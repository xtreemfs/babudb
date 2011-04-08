/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.proxy.ProxyRequestHandler;
import org.xtreemfs.babudb.replication.service.accounting.LatestLSNUpdateListener;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.transmission.TransmissionToServiceInterface;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.buffer.BufferPool;
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
    private final AtomicReference<LSN>           lastOnView = new AtomicReference<LSN>();
    
    /** interface for operation performed on BabuDB */
    private final BabuDBInterface                babuDB;
        
    /** interface to the underlying layer */
    private final TransmissionToServiceInterface transmissionInterface;
        
    /** thread safe checksum object for serialization of LogEntries to replicate */
    private final AtomicReference<CRC32>         checksum = new AtomicReference<CRC32>(new CRC32());
        
    private final ReplicationConfig              config;
    
    /**
     * @param config
     * @param babuDB
     * @param transLayer
     * 
     * @throws IOException if {@link Flease} could not be initialized.
     */
    public ServiceLayer(ReplicationConfig config, BabuDBInterface babuDB,
            TransmissionToServiceInterface transLayer) throws IOException {
        
        this.config = config;
        this.transmissionInterface = transLayer;
        this.babuDB = babuDB;
        
        // ----------------------------------
        // initialize the participants states
        // ----------------------------------
        try {
            participantsStates = new ParticipantsStates(config.getSyncN(), config.getParticipants(), 
                    transLayer);
        } catch (UnknownParticipantException e) {
            throw new IOException("The address of at least one participant could not have been " +
            		"resolved, because: " + e.getMessage());
        }
        
        // ----------------------------------
        // initialize the heartbeat
        // ----------------------------------
        heartbeatThread = new HeartbeatThread(participantsStates, config.getPort());
        
        // ----------------------------------
        // initialize replication stage
        // ----------------------------------
        replicationStage = new ReplicationStage(
                config.getBabuDBConfig().getMaxQueueLength(), heartbeatThread, this, 
                transLayer.getFileIOInterface(), babuDB, lastOnView, config.getChunkSize()); 
    }
    
    /**
     * @param <T>
     * @param receiver
     */
    public <T extends ControlLayerInterface & FleaseMessageReceiver> void init(T receiver) {
        assert (receiver != null);
        
        // ----------------------------------
        // initialize request logic for 
        // handling BabuDB remote calls
        // ----------------------------------
        ProxyRequestHandler rqCtrl = new ProxyRequestHandler(babuDB, 
                config.getBabuDBConfig().getMaxQueueLength());
        transmissionInterface.addRequestHandler(rqCtrl);
        receiver.registerProxyRequestControl(rqCtrl);
        
        // ----------------------------------
        // coin the dispatcher of the 
        // underlying layer
        // ----------------------------------
        transmissionInterface.addRequestHandler(
                new ReplicationRequestHandler(participantsStates, receiver, babuDB, 
                        replicationStage, lastOnView, config.getChunkSize(), 
                        transmissionInterface.getFileIOInterface(), 
                        config.getBabuDBConfig().getMaxQueueLength()));
        
        receiver.registerReplicationControl(replicationStage);
    }
    
/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#replicate(
     *          org.xtreemfs.babudb.log.LogEntry)
     */
    @Override
    public ReplicateResponse replicate(LogEntry le) {
        
        // update the LSN of the heartbeat
        heartbeatThread.updateLSN(le.getLSN());
        
        // replicate the entry at the slaves
        List<SlaveClient> slaves;
        try {
            slaves = participantsStates.getAvailableParticipants();
        } catch (Exception e) {
            return new ReplicateResponse(le, e);
        }  
        final ReplicateResponse result = new ReplicateResponse(le,
                slaves.size() - participantsStates.getLocalSyncN());
        
        // make the replicate call at the clients
        if (slaves.size() == 0) { 
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                    "There are no slaves available anymore! " +
                    "BabuDB runs if it would be in non-replicated mode.");
        } else {
            
            // serialize the LogEntry
            ReusableBuffer payload = null;
            synchronized(checksum) {
                CRC32 csumAlgo = checksum.get();
                try {
                    payload = le.serialize(csumAlgo);
                } finally {
                    csumAlgo.reset();
                }
            }
            
            // send the LogEntry to the other servers
            for (final SlaveClient slave : slaves) {
                slave.replicate(le.getLSN(), payload.createViewBuffer()).registerListener(
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
                        if (e.getMessage() == null) {
                            Logging.logError(Logging.LEVEL_DEBUG, this, e);
                        }
                    }
                });
            }
            
            BufferPool.free(payload);
        }    
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#synchronize(
     *          org.xtreemfs.babudb.log.SyncListener, int)
     */
    @Override
    public void synchronize(SyncListener listener, int port) throws BabuDBException, 
            InterruptedException, IOException {
            
        final int localSyncN = participantsStates.getLocalSyncN();
        List<ConditionClient> slaves = participantsStates.getConditionClients();
        if (localSyncN > 0) {
            
            // always get the latest available state of all servers
            LSN latest = null;
            Map<ClientInterface ,LSN> states = getStates(slaves, true);
        
            // getting enough slaves has failed
            if (states.size() < localSyncN) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                        "Not enough slaves available to synchronize with!");
                
            } else {
                
                List<LSN> values = new LinkedList<LSN>(states.values());
                Collections.sort(values, Collections.reverseOrder());
                latest = values.get(0);
            }
        
            // synchronize with the most up-to-date slave, if necessary
            LSN localState = babuDB.getState();
            if (localState.compareTo(latest) < 0) {
                for (Entry<ClientInterface, LSN> entry : states.entrySet()) {
                    if (entry.getValue().equals(latest)) {   
                                
                        Logging.logMessage(Logging.LEVEL_INFO, this, 
                                "Starting synchronization from '%s' to '%s'.", 
                                localState.toString(), latest.toString());
                    
                        BabuDBRequestResultImpl<Object> ready = 
                            new BabuDBRequestResultImpl<Object>();
                        
                        replicationStage.manualLoad(ready, latest);
                        ready.get();
                    
                        assert(latest.equals(babuDB.getState())) : 
                            "Synchronization failed: (expected=" + 
                            latest.toString() + ") != (acknowledged=" + 
                            babuDB.getState() + ")";
                        break;
                    }
                }
            }
        }
        
        // take a checkpoint on master-failover (inclusive viewId incrementation), if necessary
        LSN beforeCP = lastOnView.get();
        if (babuDB.getState().getSequenceNo() > 0L) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "taking a checkpoint");
            beforeCP = babuDB.checkpoint();
            lastOnView.set(beforeCP);
        }
        
        // wait for the slaves to recognize the master-change and for at least N servers to 
        // establish a stable state
        LSN syncState = babuDB.getState();
        final ReplicateResponse result = new ReplicateResponse(syncState, listener,
                slaves.size() - localSyncN);
        
        for (ConditionClient slave : slaves) {
            slave.synchronize(beforeCP, port).registerListener(
                    new ClientResponseAvailableListener<Object>() {

                        @Override
                        public void responseAvailable(Object r) { /* ignored */ }

                        @Override
                        public void requestFailed(Exception e) {
                            result.decrementPermittedFailures();
                        }
            });
        }
        
        subscribeListener(result);
                
        Logging.logMessage(Logging.LEVEL_INFO, this, "Running in master-mode (%s)", 
                syncState.toString());
    }
  
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.SlaveView#getSynchronizationPartner(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public MasterClient getSynchronizationPartner(LSN progressAtLeast) {
        
        List<ConditionClient> servers = participantsStates.getConditionClients();
        
        if (servers.size() > 0) {
            Map<ClientInterface, LSN> states = getStates(servers, false);
            
            for (Entry<ClientInterface, LSN> e : states.entrySet()) {
                if (e.getValue().compareTo(progressAtLeast) >= 0)
                    return (MasterClient) e.getKey();
            }
        } else {
            return null;
        }
        
        return (MasterClient) servers.get(0);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#getParticipantOverview()
     */
    @Override
    public ParticipantsOverview getParticipantOverview() {
        return participantsStates;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#reset()
     */
    @Override
    public void reset() {
        participantsStates.reset();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#_setLifeCycleListener(org.xtreemfs.foundation.LifeCycleListener)
     */
    @Override
    public void _setLifeCycleListener(LifeCycleListener listener) {
        this.heartbeatThread.setLifeCycleListener(listener);
        this.replicationStage.setLifeCycleListener(listener);
    }
    
    /**
     * Method to initialize all ServiceLayer services. Passes the userService to be available at the
     * {@link ReplicationStage}.
     * 
     * @param userService
     */
    public void start(ControlLayerInterface topLayer) {
        LSN latest = babuDB.getState();
        LSN normalized = (latest.getSequenceNo() == 0L) ? new LSN(0,0L) : latest;
            
        // the sequence number of the initial LSN before incrementing the viewID must not be 0 
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Setting last on view " +
           "LSN to '%s', initial was '%s'.", normalized, latest);
        
        lastOnView.set(normalized);
                
        try {
            heartbeatThread.start(latest);
            replicationStage.start(topLayer);
            heartbeatThread.waitForStartup();
            replicationStage.waitForStartup();
        } catch (Exception e) {
            listener.crashPerformed(e);
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
     * @see org.xtreemfs.babudb.replication.service.ServiceToControlInterface#subscribeListener(
     *          org.xtreemfs.babudb.replication.service.accounting.LatestLSNUpdateListener)
     */
    @Override
    public void subscribeListener(LatestLSNUpdateListener rp) {
        participantsStates.subscribeListener(rp);
    }
    
/*
 * private methods
 */
    
    /**
     * Performs a network broadcast to get the latest LSN from every available DB.
     * 
     * @param clients
     * @param hard - decide whether the requested state should be preserved (true), or not (false).
     * @return the LSNs of the latest written LogEntries for the <code>babuDBs
     *         </code> that were available (could be empty).
     */
    private Map<ClientInterface, LSN> getStates(List<ConditionClient> clients, boolean hard) {
        Map<ClientInterface, LSN> result = new HashMap<ClientInterface, LSN>();
        int numRqs = clients.size();
        
        if (numRqs == 0) {
            return result;
        }
        
        List<ClientResponseFuture<LSN, org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN>> rps = 
            new ArrayList<ClientResponseFuture<LSN, org.xtreemfs.babudb.pbrpc.GlobalTypes.LSN>>(
                    numRqs);
        
        if (numRqs > 0) {
            // send the requests
            for (int i = 0; i < numRqs; i++) {
                if (hard) {
                    rps.add(i, clients.get(i).state());
                } else {
                    rps.add(i, clients.get(i).volatileState());
                }
            }
            
            // get the responses
            for (int i = 0; i < numRqs; i++) {
                try{
                    LSN val = rps.get(i).get();
                    result.put(clients.get(i), val);
                } catch (Exception e) {
                    Logging.logMessage(Logging.LEVEL_INFO, this, 
                            "Could not receive state of '%s', because: %s.", 
                            clients.get(i), e.getMessage());
                    if (e.getMessage() == null)
                        Logging.logError(Logging.LEVEL_INFO, this, e);
                } 
            }
        }
        
        return result;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.Layer#start()
     */
    @Override
    public void start() {
        throw new UnsupportedOperationException("Use start(LockableService userService) instead!");
    }
}