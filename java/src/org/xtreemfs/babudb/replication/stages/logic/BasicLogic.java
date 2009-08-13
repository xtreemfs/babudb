/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.operations.Operation;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.StageRequest;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;

import static org.xtreemfs.babudb.replication.stages.ReplicationStage.*;

/**
 * <p>Performs the basic replication {@link Operation}s on the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class BasicLogic extends Logic {

    private final AtomicBoolean quit = new AtomicBoolean(false);
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
        });
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return LogicID.BASIC;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        quit.set(false);
        
        while (!quit.get()) {
            try {
                final StageRequest op = queue.take();
                final LSN lsn = op.getLSN();
                
                // check the LSN of the logEntry to write
                if (lsn.getViewId() == stage.loadUntil.getViewId()) {
                    if (lsn.getSequenceNo() <= stage.loadUntil.getSequenceNo()) {
                        stage.finalizeRequest(op);
                        continue;
                    } else if (lsn.getSequenceNo() > stage.loadUntil.getSequenceNo()+1) {
                        queue.add(op);
                        quit.set(true);
                        request(new LSNRange(lsn.getViewId(),stage.loadUntil.getSequenceNo()+1,lsn.getSequenceNo()-1));
                        break;
                    }
                } else if(lsn.getViewId() < stage.loadUntil.getViewId()){
                    stage.finalizeRequest(op);
                    continue;
                } else throw new Exception("Slave is out of sync...");
                
                switch (op.getStageMethod()) {
                
                case REPLICATE :            
                    LogEntry le = (LogEntry) op.getArgs()[1];
                    // prepare the request
                    try {
                        LSMDBRequest rq = SharedLogic.retrieveRequest(le, new SimplifiedBabuDBRequestListener() {
                        
                            @Override
                            void finished(Object context, BabuDBException error) {
                                if (error == null) stage.dispatcher.updateLatestLSN(lsn); 
                                stage.finalizeRequest(op);
                            }
                        }, this, stage.dispatcher.db.getDatabaseManager().getDatabaseMap());
                        
                        // start the request
                        SharedLogic.writeLogEntry(rq, stage.dispatcher.db);
                    } catch (IOException io) {
                      throw io;  
                    } 
                    break;
                    
                case CREATE : 
                    stage.dispatcher.db.logger.append(noOp);
                    stage.dispatcher.db.getDatabaseManager().proceedCreate((String) op.getArgs()[1], (Integer) op.getArgs()[2], null);
                    stage.dispatcher.updateLatestLSN(lsn);
                    stage.finalizeRequest(op);
                    break;
                    
                case COPY :
                    stage.dispatcher.db.logger.append(noOp);
                    stage.dispatcher.db.getDatabaseManager().proceedCopy((String) op.getArgs()[1], (String) op.getArgs()[2], null, null);
                    stage.dispatcher.updateLatestLSN(lsn);
                    stage.finalizeRequest(op);
                    break;
                    
                case DELETE :
                    stage.dispatcher.db.logger.append(noOp);
                    stage.dispatcher.db.getDatabaseManager().proceedDelete((String) op.getArgs()[1], (Boolean) op.getArgs()[2]);
                    stage.dispatcher.updateLatestLSN(lsn);
                    stage.finalizeRequest(op);
                    break;
                    
                default : assert(false);
                    stage.finalizeRequest(op);
                    break;
                }
                
                stage.loadUntil = lsn;
            } catch (InterruptedException i) {
                Logging.logMessage(Logging.LEVEL_INFO, this, "Leaving basic-replication mode, because: %s", i.getMessage());
                quit.set(true);
            } catch (Exception e) {
                Logging.logError(Logging.LEVEL_INFO, this, e);
                stage.setCurrentLogic(LogicID.LOAD);
                quit.set(true);
            }
        }
    }

    /**
     * Switches to request-mode registering range at the missingRange field.
     * @param range
     */
    private synchronized void request(LSNRange range) {
        if (stage.setMissing(range) && stage.setCurrentLogic(LogicID.REQUEST)) {
            if (!quit.get()) stage.interrupt();
        }
    }
}