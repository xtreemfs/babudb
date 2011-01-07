/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import java.util.concurrent.BlockingQueue;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.Pacemaker;
import org.xtreemfs.babudb.replication.service.ReplicationStage;
import org.xtreemfs.babudb.replication.service.ReplicationStage.Range;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.replication.service.logic.LogicID.*;

/**
 * <p>Performs the basic replication operations on the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class BasicLogic extends Logic {
    
    /** queue containing all incoming requests */
    private final BlockingQueue<StageRequest>   queue;
    
    /**
     * @param stage
     * @param pacemaker
     * @param slaveView
     * @param fileIO
     * @param babuInterface
     * @param q
     */
    public BasicLogic(ReplicationStage stage, Pacemaker pacemaker, 
            SlaveView slaveView, FileIOInterface fileIO, 
            BabuDBInterface babuInterface, BlockingQueue<StageRequest> q) {
        super(stage, pacemaker, slaveView, fileIO, babuInterface);
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
        // filter dummy stage request
        if (op.getLSN() == null) return;
        
        final LSN lsn = op.getLSN();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Replicate requested: %s", 
                lsn.toString());
        
        LSN expected = new LSN(stage.lastInserted.getViewId(), 
                               stage.lastInserted.getSequenceNo() + 1L);
        
        // check the LSN of the logEntry to write
        if (lsn.compareTo(stage.lastInserted) <= 0) {
            // entry was already inserted
            stage.finalizeRequest(op);
            return;
        } else if(!lsn.equals(expected)){
            // we missed one or more entries
            queue.add(op);
            stage.missing = new Range(stage.lastInserted, lsn);
            stage.setLogic(REQUEST, "We missed some LogEntries from " +
                    stage.lastInserted.toString() + " to "+ lsn.toString() + 
                    ".");
            return;
        } 
        
        // try to finish the request
        try {     
            // perform the operation
            this.slaveView.handleLogEntry((LogEntry) op.getArgs()[1], new SyncListener() {
                
                @Override
                public void synced(LogEntry entry) {
                    stage.finalizeRequest(op);
                    pacemaker.updateLSN(lsn);
                }
            
                @Override
                public void failed(LogEntry entry, Exception ex) {
                    stage.finalizeRequest(op);
                    stage.lastInserted = new LSN(lsn.getViewId(),
                            lsn.getSequenceNo()-1L);
                    Logging.logError(Logging.LEVEL_ERROR, stage, ex);
                }
            });
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