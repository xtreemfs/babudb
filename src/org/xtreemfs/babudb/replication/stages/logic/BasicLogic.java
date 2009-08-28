/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.BlockingQueue;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.SimplifiedBabuDBRequestListener;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.include.common.logging.Logging;

import static org.xtreemfs.babudb.log.LogEntry.*;
import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;

/**
 * <p>Performs the basic replication {@link Operation}s on the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class BasicLogic extends Logic {
    
    /** queue containing all incoming requests */
    private final BlockingQueue<StageRequest>      queue;
    
    public BasicLogic(ReplicationStage stage, BlockingQueue<StageRequest> q) {
        super(stage);
        this.queue = q;
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return BASIC;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() throws InterruptedException {     
        
        final StageRequest op = queue.take();
        final LSN lsn = op.getLSN();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Replicate requested: %s", lsn.toString());
        
        // check the LSN of the logEntry to write
        if (lsn.getViewId() > stage.lastInserted.getViewId()) {
            queue.add(op);
            stage.setLogic(LOAD, "We had a viewID incrementation.");
            return;
        } else if(lsn.getViewId() < stage.lastInserted.getViewId()){
            stage.finalizeRequest(op);
            return;
        } else {
            if (lsn.getSequenceNo() <= stage.lastInserted.getSequenceNo()) {
                stage.finalizeRequest(op);
                return;
            } else if (lsn.getSequenceNo() > stage.lastInserted.getSequenceNo()+1) {
                queue.add(op);
                stage.missing = new LSNRange(lsn.getViewId(),
                        stage.lastInserted.getSequenceNo()+1,lsn.getSequenceNo()-1);
                stage.setLogic(REQUEST, "We missed some LogEntries "+stage.missing.toString()+".");
                return;
            } else { 
                /* continue execution */ 
            }
        }
        
        // try to finish the request
        try {     
            LogEntry le = (LogEntry) op.getArgs()[1];
            // perform the operation
            if (le.getPayloadType() == PAYLOAD_TYPE_INSERT) {
                // prepare the LSMDBrequest
                LSMDBRequest rq = SharedLogic.retrieveRequest(le, new SimplifiedBabuDBRequestListener() {
                
                    @Override
                    public void finished(Object context, BabuDBException error) {
                        if (error == null) stage.dispatcher.updateLatestLSN(lsn); 
                        stage.finalizeRequest(op);
                    }
                }, this, ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).
                                                    getDatabaseMap());
                
                // start the LSMDBrequest
                SharedLogic.writeLogEntry(rq, stage.dispatcher.dbs);
            } else {          
                // check the payload type
                switch (le.getPayloadType()) {                      
                case PAYLOAD_TYPE_CREATE:
                    // deserialize the create call
                    int indices = le.getPayload().getInt();
                    String dbName = le.getPayload().getString();
                    ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).
                        proceedCreate(dbName, indices, null);
                    break;
                    
                case PAYLOAD_TYPE_COPY:
                    // deserialize the copy call
                    String dbSource = le.getPayload().getString();
                    dbName = le.getPayload().getString();
                    ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).
                        proceedCopy(dbSource, dbName);
                    break;
                    
                case PAYLOAD_TYPE_DELETE:
                    // deserialize the create operation call
                    dbName = le.getPayload().getString();
                    boolean delete = le.getPayload().getBoolean();
                    ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).
                        proceedDelete(dbName, delete);
                    break;
                    
                case PAYLOAD_TYPE_SNAP:
                    ObjectInputStream oin = null;
                    try {
                        oin = new ObjectInputStream(new ByteArrayInputStream(le.getPayload().
                                array()));
                        // deserialize the snapshot configuration
                        int dbId = oin.readInt();
                        SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                        ((SnapshotManagerImpl) stage.dispatcher.dbs.getSnapshotManager()).
                            createPersistentSnapshot(((DatabaseManagerImpl) stage.dispatcher.
                                    dbs.getDatabaseManager()).getDatabase(dbId).getName(), 
                                    snap, false);
                    } catch (Exception e) {
                        throw new IOException("Could not deserialize operation of type "+
                                le.getPayloadType()+", because: "+e.getMessage(), e);
                    } finally {
                        if (oin != null) oin.close();
                    }
                    break;
                        
                default: new IOException("unknown payload-type");
                }
                le.getPayload().flip();
                le.setListener(new SyncListener() {
                
                    @Override
                    public void synced(LogEntry entry) {
                        stage.finalizeRequest(op);
                        stage.dispatcher.updateLatestLSN(lsn);
                    }
                
                    @Override
                    public void failed(LogEntry entry, Exception ex) {
                        stage.finalizeRequest(op);
                        stage.lastInserted = new LSN(lsn.getViewId(),lsn.getSequenceNo()-1L);
                        Logging.logError(Logging.LEVEL_ERROR, stage, ex);
                    }
                });
                
                // append logEntry to the logFile
                stage.dispatcher.dbs.getLogger().append(le);  
            }
            stage.lastInserted = lsn;
        } catch (BabuDBException e) {
            // the insert failed due an DB error
            queue.add(op);
            stage.setLogic(LOAD, e.getMessage());            
        } catch (IOException c) {
            // request could not be decoded --> it is rejected
            Logging.logMessage(Logging.LEVEL_WARN, this, "Damaged request received: %s", c.getMessage());
            stage.finalizeRequest(op);  
        } catch (InterruptedException i) {
            // crash or shutdown signal received
            stage.finalizeRequest(op);
            throw i;
        }
    }
}