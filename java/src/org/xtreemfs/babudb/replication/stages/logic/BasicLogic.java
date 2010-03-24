/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.logging.Logging;

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
        assert (stage.missing == null) : "Blame the developer!";
        
        final StageRequest op = queue.take();
        if (op.getLSN() == null) return;
        final LSN lsn = op.getLSN();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Replicate requested: %s", 
                lsn.toString());
        
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
                        stage.lastInserted.getSequenceNo()+1,lsn.getSequenceNo());
                stage.setLogic(REQUEST, "We missed some LogEntries "+
                        stage.missing.toString()+".");
                return;
            } else { 
                /* continue execution */ 
            }
        }
        
        // try to finish the request
        try {     
            // perform the operation
            SharedLogic.handleLogEntry((LogEntry) op.getArgs()[1], new SyncListener() {
                
                @Override
                public void synced(LogEntry entry) {
                    stage.finalizeRequest(op);
                    stage.dispatcher.updateLatestLSN(lsn);
                }
            
                @Override
                public void failed(LogEntry entry, Exception ex) {
                    stage.finalizeRequest(op);
                    stage.lastInserted = new LSN(lsn.getViewId(),
                            lsn.getSequenceNo()-1L);
                    Logging.logError(Logging.LEVEL_ERROR, stage, ex);
                }
            }, stage.dispatcher.dbs);
            stage.lastInserted = lsn;
        } catch (BabuDBException e) {
            // the insert failed due a DB error
            queue.add(op);
            stage.setLogic(LOAD, e.getMessage());            
        } catch (InterruptedException i) {
            // crash or shutdown signal received
            queue.add(op);
            throw i;
        }
    }
}