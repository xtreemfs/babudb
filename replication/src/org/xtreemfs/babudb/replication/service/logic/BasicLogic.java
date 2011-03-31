/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
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
        
    /** LSN of the last entry asynchronously inserted */
    private final AtomicReference<LSN>          lastAsyncInserted = new AtomicReference<LSN>(null);
    
    /**
     * @param stage
     * @param pacemaker
     * @param slaveView
     * @param fileIO
     * @param q
     */
    public BasicLogic(ReplicationStage stage, Pacemaker pacemaker, SlaveView slaveView, 
            FileIOInterface fileIO) {
        super(stage, pacemaker, slaveView, fileIO);
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
        
        final StageRequest op = stage.getQueue().take();
        
        // filter dummy stage request
        if (op == StageRequest.NOOP_REQUEST) return;
        
        final LSN lsn = op.getLSN();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Replicate requested: %s", 
                lsn.toString());
        
        LSN lastAsync = lastAsyncInserted.get();
        LSN actual = stage.getBabuDB().getState();
        actual = (lastAsync == null || actual.compareTo(lastAsync) >= 0) ? actual : lastAsync;
        LSN expected = new LSN(actual.getViewId(), actual.getSequenceNo() + 1L);
        
        // check the LSN of the logEntry to write
        if (lsn.compareTo(actual) <= 0) {
            // entry was already inserted
            stage.finalizeRequest(op);
            return;
        } else if(!lsn.equals(expected)){
            // we missed one or more entries
            stage.getQueue().add(op);
            stage.missing = new Range(actual, lsn);
            stage.setLogic(REQUEST, "We missed some LogEntries from " +
                    actual.toString() + " to "+ lsn.toString() + ".");
            return;
        } 
        
        // try to finish the request
        try {     
            // perform the operation
            stage.getBabuDB().appendToLocalPersistenceManager((LogEntry) op.getArgs()[1], 
                    new DatabaseRequestListener<Object>() {
                        
                        @Override
                        public void finished(Object result, Object context) {
                            stage.finalizeRequest(op);
                            pacemaker.updateLSN(lsn);
                        }
                        
                        @Override
                        public void failed(BabuDBException error, Object context) {
                            lastAsyncInserted.set(null);
                            stage.finalizeRequest(op);
                            Logging.logError(Logging.LEVEL_ERROR, stage, error);
                        }
                    });
            lastAsyncInserted.set(lsn);
            
        } catch (BabuDBException e) {
            
            // the insert failed due a DB error
            stage.getQueue().add(op);
            stage.setLogic(LOAD, e.getMessage());            
        }
    }
}