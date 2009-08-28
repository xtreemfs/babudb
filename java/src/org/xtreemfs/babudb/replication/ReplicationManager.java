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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherBackupState;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.config.SlaveConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;

/**
 * <p>Configurable settings.</p>
 * <p>Used to be one single Instance.</p>
 * <p>Thread safe.</p>
 * 
 * @since 06/05/2009
 * @author flangner
 */

public class ReplicationManager {
    
    private boolean isMaster;
    private RequestDispatcher dispatcher;
    private final BabuDB dbs;
    private volatile boolean stopped = true;
    
    /**
     * For setting up the {@link BabuDB} with master-replication. 
     * 
     * @param config
     * @param db
     * @param initial
     * @throws IOException
     */
    public ReplicationManager(MasterConfig config, BabuDB db, LSN initial) throws IOException {
        this.dbs = db;
        this.isMaster = true;
        TimeSync.initialize(config.getLocalTimeRenew());
        this.dispatcher = new MasterRequestDispatcher(config, db, initial);
    }
    
    /**
     * For setting up the {@link BabuDB} with slave-replication. 
     * 
     * @param config
     * @param db
     * @param initial
     * @throws IOException
     */
    public ReplicationManager(SlaveConfig config, BabuDB db, LSN initial) throws IOException {
        this.dbs = db;
        this.isMaster = false;
        TimeSync.initialize(config.getLocalTimeRenew());
        this.dispatcher = new SlaveRequestDispatcher(config, db, initial);
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
        
        if (!isMaster) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                "This BabuDB is not running in master-mode! The operation is not available.");
        else if (stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                "Replication is disabled at the moment!");
        else {
            try {
                dispatcher.replicate(le);
            } catch (Exception e){
                le.getAttachment().getListener().requestFailed(le.getAttachment().
                        getContext(), new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                                "A LogEntry could not be replicated because: "+e.getMessage()));
            } 
        }
    }
    
    /**
     * <p>Performs a network broadcast to get the latest LSN from every available DB.</p>
     * 
     * @param babuDBs
     * @return the LSNs of the latest written LogEntries for all given <code>babuDBs</code>.
     * @throws Exception if request could not be proceed.
     */
    @SuppressWarnings("unchecked")
    public Map<InetSocketAddress,LSN> getStates(List<InetSocketAddress> babuDBs) throws Exception {
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
        return this.isMaster;
    }

    /**
     * Starts the stages if available.
     */
    public void initialize() {
        dispatcher.start();
        stopped = false;
    }
       
    /**
     * <p>Stops all currently running replication operations.
     * Incoming requests will be rejected, even if the replication
     * was running in master-mode and the request was a user operation,
     * like inserts or create, copy, delete operation.</p>
     * 
     * @return dispatcher backup state.
     */
    public DispatcherBackupState stop() {
        stopped = true;
        return dispatcher.stop();
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
     * @see ReplcationManager.stop()
     */
    public void changeConfiguration(ReplicationConfig config, DispatcherBackupState lastState) throws IOException, BabuDBException {
        if (!stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "The replication has to be stopped, before the configuration can be changed!");
        
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
        stopped = true;
        dispatcher.shutdown();
    }
    
/*
 * private methods
 */

    /**
     * <p>Sets the replication service in addition to change the configuration.
     * Replication must not be running!</p>
     * 
     * @param config
     * @param lastState
     * @throws IOException 
     */
    private void set(MasterConfig config, DispatcherBackupState lastState) throws IOException {
        assert(lastState!=null) : "Use the constructor instead";
        isMaster = true;
        dispatcher = new MasterRequestDispatcher(config, dbs, lastState);
        initialize();
    }
    
    /**
     * <p>Sets the replication service in addition to change the configuration.
     * Replication must not be running!</p>
     * 
     * @param config
     * @param lastState
     * @throws IOException 
     */
    private void set(SlaveConfig config, DispatcherBackupState lastState) throws IOException {
        isMaster = false;
        DirectFileIO.replayBackupFiles(config);
        dispatcher = new SlaveRequestDispatcher(config, dbs, lastState);
        initialize();
    }
}
