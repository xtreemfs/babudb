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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.SimplifiedBabuDBRequestListener;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;

/**
 * <p>Configurable settings.</p>
 * <p>Used to be one single Instance.</p>
 * <p>Thread safe.</p>
 * 
 * @since 06/05/2009
 * @author flangner
 */

public class ReplicationManagerImpl implements ReplicationManager {
    
    private RequestDispatcher   dispatcher;
    
    /**
     * <p>For setting up the {@link BabuDB} with replication. 
     * Replication instance will be remaining stopped and in slave-mode.</p>
     * 
     * @param dbs
     * @param initial
     * @throws IOException
     * @throws InterruptedException 
     */
    public ReplicationManagerImpl(BabuDB dbs, LSN initial) 
        throws IOException, InterruptedException {
        TimeSync.initialize(((ReplicationConfig) dbs.getConfig()).
                getLocalTimeRenew());
        this.dispatcher = new SlaveRequestDispatcher(dbs, 
                new DispatcherState(initial));
    }
    
/*
 * Interface for BabuDB
 */
    
    /**
     * <p>Approach for a Worker to announce a new {@link LogEntry} <code>le</code> to the {@link ReplicationThread}.</p>
     * 
     * @param le
     * @throws BabuDBException if this is not a master.
     */
    public void replicate(LogEntry le) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: replicate...");

        try {
            dispatcher.replicate(le);
        } catch (Exception e){
            new BabuDBException(ErrorCode.REPLICATION_FAILURE, "A LogEntry" +
            		" could not be replicated because: "+e.getMessage());
        } 
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#getStates(java.util.List)
     */
    @SuppressWarnings("unchecked")
    public Map<InetSocketAddress,LSN> getStates(List<InetSocketAddress> babuDBs) {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: state...");
        
        int numRqs = babuDBs.size();
        Map<InetSocketAddress,LSN> result = new HashMap<InetSocketAddress, LSN>();
        RPCResponse<LSN>[] rps = new RPCResponse[numRqs];
        
        // send the requests
        StateClient c;
        for (int i=0;i<numRqs;i++) {
            c = new StateClient(dispatcher.rpcClient,babuDBs.get(i));
            rps[i] = (RPCResponse<LSN>) c.getState();
        }
        
        // get the responses
        for (int i=0;i<numRqs;i++) {
            try{
                LSN val = rps[i].get();
                result.put(babuDBs.get(i), val);
            } catch (Exception e) {
                result.put(babuDBs.get(i), null);
            } finally {
                if (rps[i]!=null) rps[i].freeBuffers();
            }
        }
        
        return result;
    }

    /**
     * @return true, if the replication is running in master mode. false, otherwise.
     */
    public boolean isMaster() {
        return dispatcher.isMaster;
    }

    /**
     * Starts the stages if available.
     */
    public void initialize() {
        dispatcher.start();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#declareToSlave(java.net.InetSocketAddress)
     */
    public void declareToSlave(InetSocketAddress master) 
        throws InterruptedException {
        if (!dispatcher.isMaster) return;
        
        if (isRunning()) stop();
        this.dispatcher = new SlaveRequestDispatcher(dispatcher);
        ((SlaveRequestDispatcher) this.dispatcher).coin(master);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#declareToMaster()
     */
    public void declareToMaster() throws 
        NotEnoughAvailableSlavesException, IOException, ONCRPCException, 
        InterruptedException, BabuDBException {
        
        if (dispatcher.isMaster) return;
        
        // stop the replication locally
        DispatcherState state = null;
        if (isRunning()) {
            state = stop();
        } else {
            state = dispatcher.getState();
        }
        if (state.requestQueue != null)
            for (StageRequest rq : state.requestQueue)
                rq.free();
        state.requestQueue = null;
        
        this.dispatcher = new SlaveRequestDispatcher(this.dispatcher);
        SlaveRequestDispatcher lDisp = (SlaveRequestDispatcher) dispatcher;
        
        // get a list of the registered slaves
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>(
                dispatcher.configuration.getParticipants());
        slaves.remove(dispatcher.configuration.getInetSocketAddress());
        
        if (slaves.size() > 0) {
            // stop the slaves and get their states
            Map<InetSocketAddress,LSN> states = stopAll(slaves);
            if (states.size()<dispatcher.configuration.getSyncN()) throw 
                new NotEnoughAvailableSlavesException("to get a state from");
            
            if (states.size() > 0) {                
                // if one of them is more up to date, then synchronize with it
                List<LSN> values = new LinkedList<LSN>(states.values());
                Collections.sort(values);
                LSN latest = values.get(values.size()-1);
                
                if (latest.compareTo(state.latest) > 0) {
                    for (Entry<InetSocketAddress,LSN> entry : states.entrySet()) 
                    {
                        if (entry.getValue().equals(latest)) {
                            lDisp.coin(entry.getKey());
                            StateClient c = new StateClient(
                                    dispatcher.rpcClient, entry.getKey());
                            RPCResponse<Object> rp = null;
                            try {
                                rp = c.toMaster();
                                rp.get();
                            } finally {
                                if (rp!=null) rp.freeBuffers();
                            }
                            
                            lDisp.synchronize(state.latest, latest);
                            
                            RPCResponse<LSN> rps = null;
                            try {
                                rps = c.remoteStop();
                                rps.get();
                            } finally {
                                if (rps!=null) rps.freeBuffers();
                            }
                            break;
                        }
                    }
                }
                
                
            }
            // put them all into slave-mode
            allToSlaves(slaves, dispatcher.configuration.getSyncN(), 
                    dispatcher.configuration.getInetSocketAddress());
        }
        // reset the new master
        this.dispatcher = new MasterRequestDispatcher(dispatcher, 
                dispatcher.configuration.getInetSocketAddress());        
        restart(state);
    }
       
    /**
     * <p>
     * Stops all currently running replication operations.
     * Incoming requests will be rejected, even if the replication
     * was running in master-mode and the request was a user operation,
     * like inserts or create, copy, delete operation.
     * </p>
     * 
     * @see ReplicationManager.restart()
     * 
     * @return dispatcher backup state.
     * @throws InterruptedException 
     */
    public DispatcherState stop() throws InterruptedException {
        final AtomicBoolean ready = new AtomicBoolean(false);
        dispatcher.pauses(new SimplifiedBabuDBRequestListener() {
        
            @Override
            public void finished(BabuDBException error) {
                synchronized (ready) {
                    ready.set(true);
                    ready.notify();
                }
            }
        });
        synchronized (ready) {
            if (!ready.get())
                ready.wait();
        }
        
        return dispatcher.getState();
    }
    
    /**
     * <p>
     * Continues with the replication.
     * </p>
     * 
     * @see ReplicationManager.stop()
     * 
     * @param state
     * @throws BabuDBException
     */
    public void restart(DispatcherState state) throws BabuDBException {
        dispatcher.continues(state);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#shutdown()
     */
    public void shutdown() {
        dispatcher.shutdown();
    }
    
    /**
     * Used to remotely renew the request dispatcher to change its behavior.
     * 
     * @param dispatcher
     */
    public void renewDispatcher (RequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }
    
    /**
     * 
     * @return true, if the dispatcher is running at the moment, or false,
     *              if it is disabled.
     */
    public boolean isRunning() {
        return !dispatcher.stopped;
    }
    
/*
 * private methods
 */
    
    /**
     * <p>
     * Changes the mode of at least syncN participants to slave-mode.
     * </p>
     * 
     * @param babuDBs
     * @param syncN
     * @param newMaster
     * @throws NotEnoughAvailableSlavesException
     * 
     */
    private void allToSlaves(List<InetSocketAddress> babuDBs, final int syncN, 
            InetSocketAddress newMaster) throws NotEnoughAvailableSlavesException{
                
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: toSlave...");
        int numRqs = babuDBs.size();
        if (numRqs<syncN) throw new NotEnoughAvailableSlavesException("could not proceed toSlaves broadcast");
        
        final AtomicInteger openRequests = new AtomicInteger(numRqs);
        final AtomicInteger synced = new AtomicInteger(0);
        
        StateClient c;
        for (int i=0;i<numRqs;i++) {
            c = new StateClient(dispatcher.rpcClient, babuDBs.get(i));
            c.toSlave(newMaster).registerListener(new RPCResponseAvailableListener<Object>() {
            
                @Override
                public void responseAvailable(RPCResponse<Object> r) {
                    try {
                        r.get();
                        synchronized (openRequests) {
                            openRequests.decrementAndGet();
                            if (synced.incrementAndGet() == syncN) 
                                openRequests.notify();
                        }
                    } catch (Exception e) {
                        synchronized (openRequests) {
                            if (openRequests.decrementAndGet() < (syncN-synced.get()))
                                openRequests.notify();
                        }
                    } finally {
                        r.freeBuffers();
                    }
                }
            });
        }
        
        synchronized (openRequests) {
            try {
                if (synced.get()<syncN)
                    openRequests.wait();
            } catch (InterruptedException i) {
                /* ignored */
            }
        }
        
        if (synced.get()<syncN) throw new NotEnoughAvailableSlavesException("could be declared to slaves");
    }
    
    /**
     * <p>Performs a network broadcast to stop and get the latest LSN from every available DB.</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>.
     * @throws Exception if request could not be proceed.
     */
    @SuppressWarnings("unchecked")
    private Map<InetSocketAddress,LSN> stopAll(List<InetSocketAddress> babuDBs) {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: stop...");
        
        int numRqs = babuDBs.size();
        Map<InetSocketAddress,LSN> result = new HashMap<InetSocketAddress, LSN>();
        RPCResponse<LSN>[] rps = new RPCResponse[numRqs];
        
        // send the requests
        StateClient c;
        for (int i=0;i<numRqs;i++) {
            c = new StateClient(dispatcher.rpcClient,babuDBs.get(i));
            rps[i] = (RPCResponse<LSN>) c.remoteStop();
        }
        
        // get the responses
        for (int i=0;i<numRqs;i++) {
            try{
                LSN val = rps[i].get();
                result.put(babuDBs.get(i), val);
            } catch (Exception e) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "'%s' could not" +
                		" be stopped, because: %s", babuDBs.get(i).toString(), 
                		e.getMessage());
            } finally {
                if (rps[i]!=null) rps[i].freeBuffers();
            }
        }
        
        return result;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#getMaster()
     */
    @Override
    public InetSocketAddress getMaster() {
        if (isMaster()) 
            return dispatcher.configuration.getInetSocketAddress();
        else if (((SlaveRequestDispatcher) dispatcher).master != null) 
            return ((SlaveRequestDispatcher) dispatcher).master.getDefaultServerAddress();
        else
            return null;
    }
}
