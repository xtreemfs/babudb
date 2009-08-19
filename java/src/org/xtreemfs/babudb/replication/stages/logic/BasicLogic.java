/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.util.concurrent.BlockingQueue;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;

import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;
import static org.xtreemfs.babudb.replication.stages.ReplicationStage.*;

/**
 * <p>Performs the basic replication {@link Operation}s on the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class BasicLogic extends Logic {

    private final LogEntry noOp; 
    
    /** queue containing all incoming requests */
    private final BlockingQueue<StageRequest>      queue;
    
    public BasicLogic(ReplicationStage stage, BlockingQueue<StageRequest> q) {
        super(stage);
        this.queue = q;
        this.noOp = new LogEntry(ReusableBuffer.wrap(new byte[0]),new SyncListener() {       
            @Override
            public void synced(LogEntry entry) { /* ignored */ }       
            @Override
            public void failed(LogEntry entry, Exception ex) { /* ignored */ }
        },LogEntry.PAYLOAD_TYPE_INSERT);
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
        if (lsn.getViewId() > stage.loadUntil.getViewId()) {
            queue.add(op);
            stage.setLogic(LOAD);
            return;
        } else if(lsn.getViewId() < stage.loadUntil.getViewId()){
            stage.finalizeRequest(op);
            return;
        } else {
            if (lsn.getSequenceNo() <= stage.loadUntil.getSequenceNo()) {
                stage.finalizeRequest(op);
                return;
            } else if (lsn.getSequenceNo() > stage.loadUntil.getSequenceNo()+1) {
                queue.add(op);
                stage.missing = new LSNRange(lsn.getViewId(),
                        stage.loadUntil.getSequenceNo()+1,lsn.getSequenceNo()-1);
                stage.setLogic(REQUEST);
                return;
            } else { 
                /* continue execution */ 
            }
        }
        
        // try to finish the request
        try {       
            switch (op.getStageMethod()) {
            
            case REPLICATE :            
                LogEntry le = (LogEntry) op.getArgs()[1];
                // prepare the request
                LSMDBRequest rq = SharedLogic.retrieveRequest(le, new SimplifiedBabuDBRequestListener() {
                
                    @Override
                    void finished(Object context, BabuDBException error) {
                        if (error == null) stage.dispatcher.updateLatestLSN(lsn); 
                        stage.finalizeRequest(op);
                    }
                }, this, ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).getDatabaseMap());
                
                // start the request
                SharedLogic.writeLogEntry(rq, stage.dispatcher.dbs);
                break;
                
            case CREATE : 
                stage.dispatcher.dbs.getLogger().append(noOp);
                ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).proceedCreate((String) op.getArgs()[1], (Integer) op.getArgs()[2], null);
                stage.dispatcher.updateLatestLSN(lsn);
                stage.finalizeRequest(op);
                break;
                
            case COPY :
                stage.dispatcher.dbs.getLogger().append(noOp);
                ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).proceedCopy((String) op.getArgs()[1], (String) op.getArgs()[2]);
                stage.dispatcher.updateLatestLSN(lsn);
                stage.finalizeRequest(op);
                break;
                
            case DELETE :
                stage.dispatcher.dbs.getLogger().append(noOp);
                ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).proceedDelete((String) op.getArgs()[1], (Boolean) op.getArgs()[2]);
                stage.dispatcher.updateLatestLSN(lsn);
                stage.finalizeRequest(op);
                break;
                
            default : assert(false);
                stage.finalizeRequest(op);
                break;
            }
            
            stage.loadUntil = lsn;
        } catch (BabuDBException e) {
            // the insert failed due an DB error
            Logging.logMessage(Logging.LEVEL_WARN, this, "Leaving basic-replication mode, because: %s", e.getMessage());
            stage.setLogic(LOAD);
        } 
    }
}