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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.clients.SlaveClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.SlavesStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.operations.ChunkOperation;
import org.xtreemfs.babudb.replication.operations.HeartbeatOperation;
import org.xtreemfs.babudb.replication.operations.LoadOperation;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.operations.ReplicaOperation;
import org.xtreemfs.babudb.replication.operations.StateOperation;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

/**
 * Dispatches incoming master-requests.
 * 
 * @since 05/03/2009
 * @author flangner
 * @see RequestDispatcher
 */

public class MasterRequestDispatcher extends RequestDispatcher {
    
    private final Checksum checksum = new CRC32();
    private final SlavesStates states;
    private final int syncN;
    
    public final int chunkSize;
    
    /** The last acknowledged LSN of the last view. */
    public final LSN lastOnView;
    
    /**
     * Initial setup.
     * 
     * @param config
     * @param dbs
     * @param initial
     * @throws IOException
     */
    public MasterRequestDispatcher(MasterConfig config, BabuDB dbs, LSN initial) throws IOException {
        super("Master", config, dbs);
        this.isMaster = true;
        this.syncN = config.getSyncN();
        this.chunkSize = config.getChunkSize();
        this.states = new SlavesStates(config.getSyncN(),config.getSlaves(),rpcClient);
        
        // the sequence number of the initial LSN before incrementing the viewID cannot be 0 
        this.lastOnView = (initial.getSequenceNo() == 0L) ? new LSN(0,0L) : initial;
    }
    
    /**
     * Reset configuration.
     * 
     * @param config
     * @param dbs
     * @param initial
     * @param backupState - needed if the dispatcher shall be reset. Includes the initial LSN.
     * @throws IOException
     */
    public MasterRequestDispatcher(MasterConfig config, BabuDB dbs, DispatcherState backupState) throws IOException {
        super("Master", config, dbs);
        this.syncN = config.getSyncN();
        this.chunkSize = config.getChunkSize();
        this.states = new SlavesStates(config.getSyncN(),config.getSlaves(),rpcClient);
        this.lastOnView = backupState.latest;
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
     * @throws InterruptedException 
     */
    public List<SlaveClient> getSlavesForBroadCast() throws NotEnoughAvailableSlavesException, InterruptedException {
        return states.getAvailableSlaves();
    }
    
    /**
     * <p>Marks the given slave as dead. Use this, if you get a bad 
     * response from a slave by a broadcast request.</p>
     * 
     * @param slave
     */
    public void markSlaveAsDead(SlaveClient slave) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Slave was marked as dead: ", slave.getDefaultServerAddress().toString());
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

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#replicate(org.xtreemfs.babudb.log.LogEntry)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void _replicate(final LogEntry le)
            throws NotEnoughAvailableSlavesException, InterruptedException, IOException {
        try {
            ReusableBuffer buffer = le.serialize(checksum);
            
            List<SlaveClient> slaves = getSlavesForBroadCast(); 
            
            // setup the response
            final ReplicateResponse result = new ReplicateResponse(le.getLSN(),slaves.size()-getSyncN()) {
                @Override
                public void upToDate() {
                    super.upToDate();
                    le.getAttachment().getListener().insertFinished(le.getAttachment().getContext());
                }
            };
            subscribeListener(result);
            
            // make the replicate call at the clients
            if (slaves.size() == 0) { 
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "There are no slaves available anymore! BabuDB runs if it would be in non-replicated mode.");
                if (buffer!=null) BufferPool.free(buffer); // FIXME : just for testing: replication with no slave is no replication... --> failover!
            } else {
                for (final SlaveClient slave : slaves) {
                    ((RPCResponse<Object>) slave.replicate(le.getLSN(), buffer))
                        .registerListener(new RPCResponseAvailableListener<Object>() {
                    
                        @Override
                        public void responseAvailable(RPCResponse<Object> r) {
                            // evaluate the response
                            try {
                                r.get();
                                markSlaveAsFinished(slave);
                            } catch (Exception e) {
                                result.decrementPermittedFailures();
                                markSlaveAsDead(slave);
                            } finally {
                                if (r!=null) r.freeBuffers();
                            }
                        }
                    });
                }
            }
        } finally {
            checksum.reset();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#getState()
     */
    @Override
    public DispatcherState getState() {
        return new DispatcherState(dbs.getLogger().getLatestLSN());
    }
}
