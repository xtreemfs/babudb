/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.service;

import static org.xtreemfs.babudb.log.LogEntry.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequest;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.Layer;
import org.xtreemfs.babudb.replication.control.RoleChangeListener;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.service.operations.ChunkOperation;
import org.xtreemfs.babudb.replication.service.operations.FleaseOperation;
import org.xtreemfs.babudb.replication.service.operations.HeartbeatOperation;
import org.xtreemfs.babudb.replication.service.operations.LoadOperation;
import org.xtreemfs.babudb.replication.service.operations.LocalTimeOperation;
import org.xtreemfs.babudb.replication.service.operations.ReplicaOperation;
import org.xtreemfs.babudb.replication.service.operations.ReplicateOperation;
import org.xtreemfs.babudb.replication.service.operations.StateOperation;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.TransmissionLayer;
import org.xtreemfs.babudb.replication.transmission.TransmissionToServiceInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RPCResponseAvailableListener;

/**
 * Contains the service logic for all necessary operations used by the 
 * replication. 
 *
 * @author flangner
 * @since 04/12/2010
 */
public class ServiceLayer extends Layer implements  ServiceToControlInterface, 
    SlaveView {
    
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
        this.heartbeatThread = new HeartbeatThread(
                this.participantsStates.getConditionClients());
        
        // ----------------------------------
        // initialize replication stage
        // ----------------------------------
        this.replicationStage = new ReplicationStage(config.getMaxQueueLength(), 
                this.heartbeatThread, this, transLayer.getFileIOInterface(), 
                this.babuDBInterface, this.lastOnView);
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
                ((RPCResponse<Object>) slave.replicate(le.getLSN(), 
                        payload.createViewBuffer())).registerListener(
                                new RPCResponseAvailableListener<Object>() {
                
                    @Override
                    public void responseAvailable(RPCResponse<Object> r) {
                        // evaluate the response
                        try {
                            r.get();
                            participantsStates.requestFinished(slave);
                        } catch (Exception e) {
                            participantsStates.markAsDead(slave);
                            result.decrementPermittedFailures();
                            
                            Logging.logMessage(Logging.LEVEL_INFO, this, 
                                    "'%s' was marked as dead, because %s", 
                                    slave.getDefaultServerAddress().toString(), 
                                    e.getMessage());
                            if (e.getMessage() == null)
                                Logging.logError(Logging.LEVEL_DEBUG, this, e);
                        } finally {
                            if (r!=null) r.freeBuffers();
                        }
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
        boolean snapshot = true;
        LSN localState = this.babuDBInterface.getState();
        if (latest != null && latest.compareTo(localState) > 0) {
            for (Entry<ClientInterface, LSN> entry : states.entrySet()) {
                if (entry.getValue().equals(latest)) {                 
                    // setup a slave dispatcher to synchronize the 
                    // BabuDB with a replicated participant         
                    Logging.logMessage(Logging.LEVEL_INFO, this, 
                            "Starting synchronization from '%s' to '%s'.", 
                            localState.toString(), latest.toString());
                
                    BabuDBRequest<Boolean> ready = new BabuDBRequest<Boolean>();
                    this.replicationStage.manualLoad(ready, localState, latest);
                    snapshot = ready.get();
                
                    assert(latest.equals(babuDBInterface.getState())) : 
                        "Synchronization failed: (expected=" + 
                        latest.toString() + ") != (acknowledged=" + 
                        babuDBInterface.getState() + ")";
                    break;
                }
            }
        }
    
        // make a snapshot
        if (snapshot && (latest.getSequenceNo() > 0L)) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "taking a new checkpoint");
            this.lastOnView.set(this.babuDBInterface.getState());
            // TODO maybe there are still some inserts on the DiskLogger-queue,
            // which might increment the lastOnView LSN.
            this.babuDBInterface.checkpoint();
    
            // switch the log-file only if there was have been replicated operations
            // since the last log-file-switch
        } else if (latest.getSequenceNo() > 0L){
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "switching the logfile only");
            this.lastOnView.set(this.babuDBInterface.switchLogFile());
        }
    
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
                Map<Integer, Operation> ops = initializeOperations(
                        this.transmissionInterface.getFileIOInterface(), 
                        receiver);
                this.transmissionInterface.coin(ops, this.participantsStates);
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.SlaveView#handleLogEntry(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.babudb.log.SyncListener)
     */
    @Override
    public void handleLogEntry(LogEntry entry, SyncListener listener) 
        throws BabuDBException, InterruptedException {

        // check the payload type
        switch (entry.getPayloadType()) { 
        
        case PAYLOAD_TYPE_INSERT:
            InsertRecordGroup irg = 
                InsertRecordGroup.deserialize(entry.getPayload());
            this.babuDBInterface.insertRecordGroup(irg);
            break;
        
        case PAYLOAD_TYPE_CREATE:
            // deserialize the create call
            int dbId = entry.getPayload().getInt();
            String dbName = entry.getPayload().getString();
            int indices = entry.getPayload().getInt();
            if (!this.babuDBInterface.dbExists(dbId))
                this.babuDBInterface.createDB(dbName, indices);
            break;
            
        case PAYLOAD_TYPE_COPY:
            // deserialize the copy call
            entry.getPayload().getInt(); // do not delete!
            dbId = entry.getPayload().getInt();
            String dbSource = entry.getPayload().getString();
            dbName = entry.getPayload().getString();
            if (!this.babuDBInterface.dbExists(dbId))
                this.babuDBInterface.copyDB(dbSource, dbName);
            break;
            
        case PAYLOAD_TYPE_DELETE:
            // deserialize the create operation call
            dbId = entry.getPayload().getInt();
            dbName = entry.getPayload().getString();
            if (this.babuDBInterface.dbExists(dbId))
                this.babuDBInterface.deleteDB(dbName);
            break;
            
        case PAYLOAD_TYPE_SNAP:
            ObjectInputStream oin = null;
            try {
                oin = new ObjectInputStream(new ByteArrayInputStream(
                        entry.getPayload().array()));
                // deserialize the snapshot configuration
                dbId = oin.readInt();
                SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                this.babuDBInterface.createSnapshot(dbId, snap);
            } catch (Exception e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                        "Could not deserialize operation of type "+entry.
                        getPayloadType()+", because: "+e.getMessage(), e);
            } finally {
                try {
                if (oin != null) oin.close();
                } catch (IOException ioe) {
                    /* who cares? */
                }
            }
            break;
            
        case PAYLOAD_TYPE_SNAP_DELETE:

            byte[] payload = entry.getPayload().array();
            int offs = payload[0];
            dbName = new String(payload, 1, offs);
            String snapName = new String(payload, offs + 1, 
                                         payload.length - offs - 1);
            this.babuDBInterface.deleteSnapshot(dbName, snapName);
            break;
                
        default: new BabuDBException(ErrorCode.INTERNAL_ERROR, "unknown " +
        		"payload-type");
        }
        entry.getPayload().flip();
        entry.setListener(listener);
        
        // append logEntry to the logFile
        this.babuDBInterface.appendToDisklogger(entry);
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
        this.replicationStage.lastInserted = this.babuDBInterface.getState();
        // TODO maybe there are still some inserts on the DiskLogger-queue,
        // which might increment the lastOnView LSN.
        this.participantsStates.setMaster(address);
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
        LSN latest = this.babuDBInterface.getState();
        
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
     * @param receiver - {@link FleaseMessageReceiver}.
     * @param fileIO
     * @return a table of {@link Operation}s suitable for the 
     *         {@link TransmissionLayer}.
     */
    private Map<Integer,Operation> initializeOperations(
            FileIOInterface fileIO, FleaseMessageReceiver receiver) {
        
        Map<Integer, Operation> result = new HashMap<Integer, Operation>();
        
        Operation op = new LocalTimeOperation();
        result.put(op.getProcedureId(), op);
        
        op = new FleaseOperation(receiver);
        result.put(op.getProcedureId(), op);
        
        op = new StateOperation(this.babuDBInterface);
        result.put(op.getProcedureId(), op);
        
        op = new HeartbeatOperation(this.participantsStates);
        result.put(op.getProcedureId(), op);
        
        op = new ReplicateOperation(this.replicationStage, 
                this.participantsStates);
        result.put(op.getProcedureId(),op);
        
        op = new ReplicaOperation(this.lastOnView, this.babuDBInterface, 
                fileIO);
        result.put(op.getProcedureId(),op);
        
        op = new LoadOperation(this.lastOnView, this.babuDBInterface, fileIO);
        result.put(op.getProcedureId(),op);
        
        op = new ChunkOperation();
        result.put(op.getProcedureId(),op);
        
        return result;
    }
    
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
    @SuppressWarnings("unchecked")
    private Map<ClientInterface, LSN> getStates(List<ConditionClient> clients) {
        int numRqs = clients.size();
        Map<ClientInterface, LSN> result = 
            new HashMap<ClientInterface, LSN>();
        RPCResponse<LSN>[] rps = new RPCResponse[numRqs];
        
        // send the requests
        for (int i=0; i<numRqs; i++) {
            rps[i] = (RPCResponse<LSN>) clients.get(i).state();
        }
        
        // get the responses
        for (int i=0;i<numRqs;i++) {
            try{
                LSN val = rps[i].get();
                result.put(clients.get(i), val);
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_INFO, this, 
                        "Could not receive state of '%s', because: %s.", 
                        clients.get(i), e.getMessage());
                if (e.getMessage() == null)
                    Logging.logError(Logging.LEVEL_INFO, this, e);
            } finally {
                if (rps[i]!=null) rps[i].freeBuffers();
            }
        }
        
        return result;
    }
}