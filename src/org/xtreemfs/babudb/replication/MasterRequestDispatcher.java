/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import java.util.List;
import java.util.concurrent.locks.Lock;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.clients.SlaveClient;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.SlavesStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.events.CopyEvent;
import org.xtreemfs.babudb.replication.events.CreateEvent;
import org.xtreemfs.babudb.replication.events.DeleteEvent;
import org.xtreemfs.babudb.replication.events.Event;
import org.xtreemfs.babudb.replication.events.ReplicateEvent;
import org.xtreemfs.babudb.replication.operations.ChunkOperation;
import org.xtreemfs.babudb.replication.operations.HeartbeatOperation;
import org.xtreemfs.babudb.replication.operations.LoadOperation;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.ReplicaOperation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.include.common.config.MasterConfig;

/**
 * Dispatches incoming master-requests.
 * 
 * @since 05/03/2009
 * @author flangner
 * @see RequestDispatcher
 */

public class MasterRequestDispatcher extends RequestDispatcher {
    
    private final SlavesStates states;
    private final int syncN;
    
    public final int chunkSize;
    
    /** Lock for babuDB operations */
    public final Lock contextSwitchLock;
    
    /** The last acknowledged LSN of the last view. */
    public volatile LSN lastOnView  = null;
    
    /**
     * public static final String DEFAULT_CFG_FILE = "config/master.properties";
     * (cfgFile != null) ? new ReplicationConfig(cfgFile) : new ReplicationConfig(DEFAULT_CFG_FILE))
     * 
     * @param config
     * @param db
     * @param initial
     * @throws IOException
     */
    public MasterRequestDispatcher(MasterConfig config, BabuDB db, LSN initial) throws IOException {
        super("Master", config, db);
        this.syncN = config.getSyncN();
        this.chunkSize = config.getChunkSize();
        this.contextSwitchLock = db.overlaySwitchLock.readLock();
        this.states = new SlavesStates(config.getSyncN(),config.getSlaves(),rpcClient);
        this.lastOnView = initial;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#initializeOperations()
     */
    @Override
    protected void initializeOperations() {
        Operation op = new StateOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new ReplicaOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new LoadOperation(this);
        operations.put(op.getProcedureId(),op);
        
        op = new ChunkOperation();
        operations.put(op.getProcedureId(),op);
        
        op = new HeartbeatOperation(this);
        operations.put(op.getProcedureId(),op);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#initializeEvents()
     */
    @Override
    protected void initializeEvents() {
        Event ev = new ReplicateEvent(this);
        events.put(ev.getProcedureId(), ev);
        
        ev = new CreateEvent(this);
        events.put(ev.getProcedureId(), ev);
        
        ev = new CopyEvent(this);
        events.put(ev.getProcedureId(), ev);
        
        ev = new DeleteEvent(this);
        events.put(ev.getProcedureId(), ev);
    }
    
    /**
     * If an instance has to be notified about latest {@link LSN} changes,
     * register it here.
     * 
     * @param listener
     */
    public void subscribeListener(LatestLSNUpdateListener listener){
        states.subscribeListener(listener);
    }
    
    /**
     * @return a list of available {@link SlaveClient}s.
     * @throws NotEnoughAvailableSlavesException
     */
    public List<SlaveClient> getSlavesForBroadCast() throws NotEnoughAvailableSlavesException {
        return states.getAvailableSlaves();
    }
    
    /**
     * <p>Marks the given slave as dead. Use this, if you get a bad 
     * response from a slave by a broadcast request.</p>
     * 
     * @param slave
     */
    public void markSlaveAsDead(SlaveClient slave) {
        states.markAsDead(slave);
    }
    
    /**
     * <p>Marks the given slave as request-finished.</p>
     * 
     * @param slave
     */
    public void markSlaveAsFinished(SlaveClient slave) {
        states.requestFinished(slave);
    }
    
    /**
     * @return the sync-N for N-synchronization ... for real.
     */
    public int getSyncN() {
        return syncN;
    }
    
    /**
     * Update the state of the given slave with its newest heartBeat message.
     * 
     * @param slave
     * @param lsn
     * @param receiveTime
     * @throws UnknownParticipantException
     */
    public void heartbeat(SocketAddress slave, LSN lsn, long receiveTime) throws UnknownParticipantException {
        assert (slave instanceof InetSocketAddress);
        states.update(((InetSocketAddress) slave).getAddress(), lsn, receiveTime);
    }
}
