/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.clients.SlaveClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.SlavesStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.operations.HeartbeatOperation;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
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
    
    private final SlavesStates states;
    private final int syncN;
    
    public MasterRequestDispatcher(BabuDB dbs, LSN initial, 
            ReplicationControlLayer replCtl) throws Exception {
        
        super("Master", dbs, initial, replCtl);
        
        // Because the master is already synchronized and belongs to the set
        // of N participants.
        this.syncN = (replCtl.configuration.getSyncN() > 0) ?
                      replCtl.configuration.getSyncN()-1 : 
                      replCtl.configuration.getSyncN();
                      
        List<InetSocketAddress> slaves = 
            new LinkedList<InetSocketAddress>(
                    replCtl.configuration.getParticipants());
        this.states = new SlavesStates(this.syncN,slaves,rpcClient,
                replCtl.configuration.getInetSocketAddress());
    }
    
    /**
     * Constructor to change the {@link RequestDispatcher} behavior to
     * master using the old dispatcher.
     * 
     * @param oldDispatcher
     * @param own - address of the master.
     */
    public MasterRequestDispatcher(RequestDispatcher oldDispatcher, InetSocketAddress own) {
        super("Master", oldDispatcher);
        
        DispatcherState oldState = oldDispatcher.getState();
        if (oldState.requestQueue != null)
            for (StageRequest rq : oldState.requestQueue)
                rq.free();
                
        // Because the master is already synchronized and belongs to the set
        // of N participants.
        this.syncN = (getConfig().getSyncN() > 0) ? getConfig().getSyncN()-1 : 
                      getConfig().getSyncN();
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>(
                getConfig().getParticipants());
        
        this.states = new SlavesStates(this.syncN,slaves,rpcClient,
                getConfig().getInetSocketAddress());
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#additionalOperations(java.util.Map)
     */
    @Override
    protected void additionalOperations(Map<Integer, Operation> operations) {        
        Operation op = new HeartbeatOperation(this);
        operations.put(op.getProcedureId(),op);
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
     * Update the state of the given slave with its newest heartBeat message.
     * 
     * @param slave
     * @param lsn
     * @param receiveTime
     * @throws UnknownParticipantException
     */
    public void heartbeat(SocketAddress slave, LSN lsn, long receiveTime) 
        throws UnknownParticipantException {
        
        assert (slave instanceof InetSocketAddress);
        states.update(((InetSocketAddress) slave).getAddress(), lsn, receiveTime);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#subscribeListener(org.xtreemfs.babudb.replication.LatestLSNUpdateListener)
     */
    @Override
    public void subscribeListener(LatestLSNUpdateListener listener){
        states.subscribeListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#replicate(org.xtreemfs.babudb.log.LogEntry, org.xtreemfs.include.common.buffer.ReusableBuffer)
     */
    @SuppressWarnings("unchecked")
    @Override
    ReplicateResponse _replicate(LogEntry le, ReusableBuffer payload) {
        
        List<SlaveClient> slaves;
        try {
            slaves = getSlavesForBroadCast();
        } catch (Exception e) {
            return new ReplicateResponse(le, e);
        }  
        final ReplicateResponse result = new ReplicateResponse(le,
                slaves.size() - this.syncN);
        
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
                            markSlaveAsFinished(slave);
                        } catch (Exception e) {
                            markSlaveAsDead(slave);
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
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#getState()
     */
    @Override
    public DispatcherState getState() {
        return new DispatcherState(dbs.getLogger().getLatestLSN());
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.RequestDispatcher#_suspend()
     */
    @Override
    protected void _suspend() {
        states.clearListeners();
    }
}
