/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.File;
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
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.config.SlaveConfig;
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

public class ReplicationManager {
    
    private RequestDispatcher dispatcher;
    private final BabuDB dbs;
    
    /**
     * For setting up the {@link BabuDB} with replication. 
     * 
     * @param config
     * @param dbs
     * @param initial
     * @throws IOException
     */
    public ReplicationManager(ReplicationConfig config, BabuDB dbs, LSN initial) throws IOException {
        this.dbs = dbs;
        TimeSync.initialize(config.getLocalTimeRenew());
        if (config instanceof MasterConfig)
            this.dispatcher = new MasterRequestDispatcher((MasterConfig) config, dbs, initial);
        else 
            this.dispatcher = new SlaveRequestDispatcher((SlaveConfig) config, dbs, initial);
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
            le.getAttachment().getListener().requestFailed(le.getAttachment().
                    getContext(), new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                            "A LogEntry could not be replicated because: "+e.getMessage()));
        } 
    }
    
    /**
     * <p>Performs a network broadcast to get the latest LSN from every available DB.</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>.
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
    
    /**
     * <p>
     * Makes the locally BabuDB running in master-mode.
     * </p>
     * <p>
     * Stops all declared slaves and synchronizes the local BabuDB 
     * with the latest slave.
     * Then restarts all participants in slave mode 
     * </p>
     * 
     * @param conf
     * @throws NotEnoughAvailableSlavesException 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ONCRPCException 
     */
    public void declareToMaster(MasterConfig conf) throws NotEnoughAvailableSlavesException, IOException, ONCRPCException, InterruptedException {
        if (dispatcher.isMaster) return;
        
        // stop the slaves and get their states
        Map<InetSocketAddress,LSN> states = stopAll(conf.getSlaves());
        if (states.size()<conf.getSyncN()) throw new NotEnoughAvailableSlavesException("to get a state from");
        
        // if one of them is more up to date, then synchronize with it
        List<LSN> values = new LinkedList<LSN>(states.values());
        Collections.sort(values);
        LSN latest = values.get(values.size()-1);
        String backupDir = new File(conf.getBaseDir()).getParent()+File.separator+"BACKUP";
        
        // stop the replication locally
        DispatcherState state = stop();
        if (state.requestQueue != null)
            for (StageRequest rq : state.requestQueue)
                rq.free();
        
        if (latest.compareTo(state.latest) > 0) {
            for (Entry<InetSocketAddress,LSN> entry : states.entrySet()) {
                if (entry.getValue().equals(latest)) {
                    set(new SlaveConfig(conf,entry.getKey(),backupDir), dispatcher.new DispatcherState(state.latest));
                    StateClient c = new StateClient(dispatcher.rpcClient, entry.getKey());
                    c.toMaster(conf.getMaster()).get();
                    
                    ((SlaveRequestDispatcher) dispatcher).synchronize(latest);
                    c.remoteStop().get();
                    break;
                }
            }
            state = stop();
        }
        
        // put them all into slave-mode
        allToSlaves(conf.getSlaves(), conf.getSyncN(), conf.getMaster());
        
        // reset the master
        set(conf,state);
    }
       
    /**
     * <p>
     * Stops all currently running replication operations.
     * Incoming requests will be rejected, even if the replication
     * was running in master-mode and the request was a user operation,
     * like inserts or create, copy, delete operation.
     * </p>
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
     * <p>Changes the replication configuration.</p>
     * <p>Makes a new checkPoint and increments the viewID, 
     * if a <code>config</code> is a {@link MasterConfig}, 
     * or {@link SlaveConfig}.</p>
     * 
     * <p><b>The replication has to be stopped before!</b></p>
     * 
     * @param config
     * @param lastState
     * 
     * @throws IOException
     * @throws BabuDBException 
     * @throws NotEnoughAvailableSlavesException 
     * @see ReplcationManager.stop()
     */
    public void changeConfiguration(ReplicationConfig config, DispatcherState lastState) throws IOException, BabuDBException {
        if (config instanceof MasterConfig) {
            ((CheckpointerImpl) dbs.getCheckpointer()).checkpoint(true);
            set((MasterConfig) config, lastState);
        } else if (config instanceof SlaveConfig) {
            ((CheckpointerImpl) dbs.getCheckpointer()).checkpoint(false);
            set((SlaveConfig) config, lastState);
        } else assert(false);
    } 

    /**
     * <p>Stops the replication process by shutting down the dispatcher.
     * And resetting the its state.</p>
     * 
     */
    public void shutdown() {
        dispatcher.shutdown();
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
                result.put(babuDBs.get(i), null);
            } finally {
                if (rps[i]!=null) rps[i].freeBuffers();
            }
        }
        
        return result;
    }
    
    /**
     * <p>Sets the replication service in addition to change the configuration.
     * Replication must not be running!</p>
     * 
     * @param config
     * @param lastState
     * @throws IOException 
     */
    private void set(ReplicationConfig config, DispatcherState lastState) throws IOException {
        assert(lastState!=null) : "Use the constructor instead";
        if (config instanceof MasterConfig)
            dispatcher = new MasterRequestDispatcher((MasterConfig) config, dbs, lastState);
        else {
            DirectFileIO.replayBackupFiles((SlaveConfig) config);
            dispatcher = new SlaveRequestDispatcher((SlaveConfig) config, dbs, lastState);
        }
        initialize();
    }
}
