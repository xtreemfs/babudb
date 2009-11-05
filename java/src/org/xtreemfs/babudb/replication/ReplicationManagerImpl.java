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
import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.DiskLogger.QueueEmptyListener;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.babudb.replication.RequestDispatcher.IState;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
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
                getLocalTimeRenew()).setLifeCycleListener(this);
        this.dispatcher = new SlaveRequestDispatcher(dbs, 
                new DispatcherState(initial));
    }
    
/*
 * interface
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#getMaster()
     */
    @Override
    public InetSocketAddress getMaster() {
        if (isMaster()) 
            return dispatcher.configuration.getInetSocketAddress();
        
        else if (dispatcher instanceof SlaveRequestDispatcher && 
                ((SlaveRequestDispatcher) dispatcher).master != null)
            
            return ((SlaveRequestDispatcher) dispatcher).master
                .getDefaultServerAddress();
        else
            return null;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#getStates(java.util.List)
     */
    @Override
    @SuppressWarnings("unchecked")
    public Map<InetSocketAddress,LSN> getStates(List<InetSocketAddress> babuDBs) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Performing requests: state...");
        
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
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#isMaster()
     */
    @Override
    public boolean isMaster() {
        return dispatcher.isMaster();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#declareToSlave(java.net.InetSocketAddress)
     */
    @Override
    public void declareToSlave(InetSocketAddress master) 
        throws InterruptedException {
        if (dispatcher.isSlave()) return;
        
        stop();
        this.dispatcher = new SlaveRequestDispatcher(dispatcher);
        ((SlaveRequestDispatcher) this.dispatcher).coin(master);
        this.dispatcher.continues(IState.SLAVE);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#declareToMaster()
     */
    @Override
    public void declareToMaster() throws 
        NotEnoughAvailableSlavesException, IOException, ONCRPCException, 
        InterruptedException, BabuDBException {
        
        if (isMaster()) return;
        
        // stop the replication locally
        DispatcherState state = stop();
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "BabuDB stopped LSN(%s)," +
                " Q: %d", state.latest.toString(), state.requestQueue.size());
        
        // get a list of the registered slaves
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>(
                dispatcher.configuration.getParticipants());
        slaves.remove(dispatcher.configuration.getInetSocketAddress());
        
        boolean snapshot = true;
        
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
                    // setup a slave dispatcher to synchronize the BabuDB with
                    // a replicated participant
                    SlaveRequestDispatcher lDisp;
                    this.dispatcher = lDisp 
                                    = new SlaveRequestDispatcher(this.dispatcher);
                    
                    for (Entry<InetSocketAddress,LSN> entry : states.entrySet()) 
                    {
                        if (entry.getValue().equals(latest)) {
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                                    "synchronize with '%s'", entry.getKey().toString());
                         
                            StateClient c = new StateClient(
                                    dispatcher.rpcClient, entry.getKey());
                            RPCResponse<Object> rp = null;
                            try {
                                rp = c.toMaster();
                                rp.get();
                            } finally {
                                if (rp!=null) rp.freeBuffers();
                            }
                            
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, "'%s' " +
                                        "was set into master mode.", c.getDefaultServerAddress());
                            
                            lDisp.coin(entry.getKey());
                            lDisp.continues(IState.SLAVE);
                            snapshot = lDisp.synchronize(state.latest, latest);
                            
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, "'%s': " +
                                        "DB was synchronized.", "localhost");
                            
                            RPCResponse<LSN> rps = null;
                            try {
                                rps = c.remoteStop();
                                rps.get();
                            } finally {
                                if (rps!=null) rps.freeBuffers();
                            }
                            
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                                    "'%s' was stopped.", 
                                    c.getDefaultServerAddress().toString());
                            state.latest = latest;
                            
                            break;
                        }
                    }
                }
            }
        }
        
        // reset the new master
        this.dispatcher = new MasterRequestDispatcher(dispatcher, 
                dispatcher.configuration.getInetSocketAddress());
        
        // switch the log-file
        if (snapshot) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "taking a new checkpoint");
            CheckpointerImpl cp = (CheckpointerImpl) dispatcher.dbs.getCheckpointer();
            cp.checkpoint(true);
        } else {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "switching the logfile");
            DiskLogger logger = dispatcher.dbs.getLogger();
            try {
                logger.lockLogger();
                logger.switchLogFile(true);
            } finally {
                logger.unlockLogger();
            }
        }
        
        // restart the replication for the slaves
        restart(IState.OTHER);
        
        // put them all into slave-mode
        allToSlaves(slaves, dispatcher.configuration.getSyncN(), 
                dispatcher.configuration.getInetSocketAddress());
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "'%d' slaves were asked " +
                    "to run in slave mode.", slaves.size());
        
        // restart the replication for the application
        restart(IState.MASTER);
                
        Logging.logMessage(Logging.LEVEL_INFO, this, "Running in master-mode " +
                        "(%s)", dispatcher.dbs.getLogger().getLatestLSN().toString());
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#shutdown()
     */
    @Override
    public void shutdown() {
        dispatcher.shutdown();
        TimeSync.getInstance().shutdown();
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.ReplicationManager#halt()
     */
    @Override
    public void halt() {
        dispatcher.pauses(null);
    }
    
/*
 * internal interface for BabuDB
 */
    
    /**
     * <p>Approach for a Worker to announce a new {@link LogEntry} <code>le</code> to the {@link ReplicationThread}.</p>
     * 
     * @param le - the original {@link LogEntry}.
     * @param buffer - the serialized {@link LogEntry}.
     * 
     * @throws IOException if the replication failed.
     * 
     * @return the {@link ReplicateResponse}.
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer buffer) throws IOException {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Performing requests: replicate...");

        try {
            return dispatcher.replicate(le, buffer);
        } catch (Throwable e){
            throw new IOException("A LogEntry could not be replicated because: " + 
                    e.getMessage());
        }
    }
    
    /**
     * Starts the stages if available.
     */
    public void initialize() {
        dispatcher.start();
    }
    
    /**
     * Sets the state for the dispatcher.
     * 
     * @param state
     */
    public void restart(IState state) {
        dispatcher.continues(state);
    }
    
    /**
     * <p>
     * Registers the listener for a replicate call.
     * </p>
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse response) {
        dispatcher.subscribeListener(response);
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
        
        // wait for the DiskLogger to finish the requests
        ready.set(false);
        dispatcher.dbs.getLogger().registerListener(new QueueEmptyListener() {
            
            @Override
            public void queueEmpty() {
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
        
        DispatcherState state = dispatcher.getState();
        Logging.logMessage(Logging.LEVEL_INFO, this, "Replication stopped:", 
                state.toString());
        
        return state;
    }
    
    /**
     * Used to remotely renew the request dispatcher to change its behavior.
     * 
     * @param state
     * @param dispatcher
     */
    public void renewDispatcher (RequestDispatcher dispatcher, IState state) {
        
        this.dispatcher = dispatcher;
        this.dispatcher.continues(state);
    }
    
    /**
     * 
     * @return true, if the dispatcher is running at the moment, or false,
     *              if it is disabled.
     */
    public boolean isRunning() {
        return !dispatcher.isPaused();
    }
    
/*
 * private methods
 */
    
    /**
     * <p>
     * Changes the mode of at least syncN participants to slave-mode.
     * <br>Longest duration: rpcClient->requestTimeout
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
        assert(newMaster != null);
                
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Performing requests: toSlave...");
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
                        Logging.logMessage(Logging.LEVEL_WARN, this, "Slave " +
                        		"could not be put into slave-mode: %s", 
                        		e.getMessage());
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
     * <br>Longest duration: rpcClient->requestTimeout</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>.
     * @throws Exception if request could not be proceed.
     */
    @SuppressWarnings("unchecked")
    private Map<InetSocketAddress,LSN> stopAll(List<InetSocketAddress> babuDBs) {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Performing requests: stop...");
        
        int numRqs = babuDBs.size();
        Map<InetSocketAddress,LSN> result = new HashMap<InetSocketAddress, LSN>();
        RPCResponse<LSN>[] rps = new RPCResponse[numRqs];
        
        // send the requests
        StateClient c;
        for (int i=0;i<numRqs;i++) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "Stopping %s", babuDBs.get(i).toString());
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
 * LifeCycleListener for the TimeSync-Thread
 */
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.include.foundation.LifeCycleListener#crashPerformed()
     */
    @Override
    public void crashPerformed() {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "TimeSync crashed!");
    }

    @Override
    public void shutdownPerformed() {
        Logging.logMessage(Logging.LEVEL_INFO, this, "TimeSync successfully %s.", "stopped");
    }

    @Override
    public void startupPerformed() {
        Logging.logMessage(Logging.LEVEL_INFO, this, "TimeSync successfully %s.", "started");
    }
}
