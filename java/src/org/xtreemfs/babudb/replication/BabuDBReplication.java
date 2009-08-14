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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherBackupState;
import org.xtreemfs.babudb.replication.trigger.CopyTrigger;
import org.xtreemfs.babudb.replication.trigger.CreateTrigger;
import org.xtreemfs.babudb.replication.trigger.DeleteTrigger;
import org.xtreemfs.babudb.replication.trigger.ReplicateTrigger;
import org.xtreemfs.include.common.TimeSync;
import org.xtreemfs.include.common.config.MasterConfig;
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

public class BabuDBReplication {
    
    private boolean isMaster;
    private RequestDispatcher dispatcher;
    private final Checksum checksum = new CRC32();
    private final BabuDB db;
    private volatile boolean stopped = true;
    
    /**
     * For setting up the {@link BabuDB} with master-replication. 
     * 
     * @param config
     * @param db
     * @param initial
     * @throws IOException
     */
    public BabuDBReplication(MasterConfig config, BabuDB db, LSN initial) throws IOException {
        this.db = db;
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
    public BabuDBReplication(SlaveConfig config, BabuDB db, LSN initial) throws IOException {
        this.db = db;
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
        
        if (!isMaster) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "This BabuDB is not running in master-mode! The operation is not available.");
        else if (stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Replication is disabled at the moment!");
        else {
            try {
                dispatcher.receiveEvent(new ReplicateTrigger(
                        le,le.getAttachment().getListener(),
                        le.getAttachment().getContext(),checksum));
            } catch (Exception e){
                le.getAttachment().getListener().requestFailed(le.getAttachment().getContext(), 
                        new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                                "A LogEntry could not be replicated because: "+e.getMessage()));
            } finally {
                checksum.reset();
            }
        }
    }
    
    /**
     * <p>Send a create request to all available slaves.</p>
     * <p>N-synchronous function.</p>
     * 
     * @param lsn
     * @param databaseName
     * @param numIndices
     * @throws BabuDBException if create request could not be replicated.
     */
    public void create(LSN lsn, String databaseName, int numIndices) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: create...");
        if (!isMaster) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,"This BabuDB is not running in master-mode! The operation is not available.");
        else if (stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Replication is disabled at the moment!");
        else {
            try {
                dispatcher.receiveEvent(new CreateTrigger(lsn,databaseName,numIndices));
            } catch (Exception e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage());
            }
        }
    }
    
    /**
     * <p>Send a copy request to all available slaves.</p>
     * <p>Synchronous function, because if there are any inconsistencies in this case,
     * concerned slaves will produce a lot of errors and will never be consistent again.</p>
     * 
     * @param lsn
     * @param sourceDB
     * @param destDB
     * @throws BabuDBException if create request could not be replicated.
     */
    public void copy(LSN lsn, String sourceDB, String destDB) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: copy...");
        if (!isMaster) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,"This BabuDB is not running in master-mode! The operation is not available.");
        else if (stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Replication is disabled at the moment!");
        else {
            try {
                dispatcher.receiveEvent(new CopyTrigger(lsn,sourceDB,destDB));
            } catch (Exception e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage());
            }
        }
    }
    
    /**
     * <p>Send a delete request to all available slaves.</p>
     * <p>Synchronous function, because if there are any inconsistencies in this case,
     * concerned slaves will produce a lot of errors and will never be consistent again.</p>
     * 
     * @param lsn
     * @param databaseName
     * @param deleteFiles
     * @throws BabuDBException if create request could not be replicated.
     */
    public void delete(LSN lsn, String databaseName, boolean deleteFiles) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_NOTICE, this, "Performing requests: delete...");
        if (!isMaster) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,"This BabuDB is not running in master-mode! The operation is not available.");
        else if (stopped) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Replication is disabled at the moment!");
        else {
            try {
                dispatcher.receiveEvent(new DeleteTrigger(lsn,databaseName,deleteFiles));
            } catch (Exception e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,e.getMessage());
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
     * Stops the replication process by shutting down the dispatcher.
     * 
     * @return dispatcher backup state.
     */
    public DispatcherBackupState shutdown() {
        stopped = true;
        dispatcher.shutdown();
        return dispatcher.getBackupState();
    }

    /**
     * <p>Sets the replication service in addition to change the configuration.
     * Replication must not be running!</p>
     * 
     * @param config
     * @throws IOException 
     * @throws InterruptedException 
     */
    public void set(MasterConfig config) throws IOException, InterruptedException {
        isMaster = true;
        dispatcher = new MasterRequestDispatcher(config, db, dispatcher.getBackupState());
        initialize();
    }
    
    /**
     * <p>Sets the replication service in addition to change the configuration.
     * Replication must not be running!</p>
     * 
     * @param config
     * @throws IOException 
     */
    public void set(SlaveConfig config) throws IOException {
        isMaster = false;
        DirectFileIO.replayBackupFiles(config);
        dispatcher = new SlaveRequestDispatcher(config, db, dispatcher.getBackupState());
        initialize();
    }
}
