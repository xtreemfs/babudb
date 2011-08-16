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
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.Pacemaker;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>Performs the basic replication operations on the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class BasicLogic extends Logic {
    
    /** Interface to the Heartbeat to update the LSN on successful replication of a LogEntry */
    private final Pacemaker pacemaker;
    
    /** 
     * @param babuDB
     * @param slaveView
     * @param fileIO
     * @param lastOnView
     * @param pacemaker
     */
    public BasicLogic(BabuDBInterface babuDB, SlaveView slaveView, FileIOInterface fileIO, 
            AtomicReference<LSN> lastOnView, Pacemaker pacemaker) {
        super(babuDB, slaveView, fileIO, lastOnView);
        
        this.pacemaker = pacemaker;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return LogicID.BASIC;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.logic.Logic#
     *      run(org.xtreemfs.babudb.replication.service.StageRequest)
     */
    @Override
    public StageCondition run(final StageRequest rq) {
                        
        final LSN lsn = rq.getLSN();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Replicate requested: %s", lsn.toString());
        
        LSN actual = getState();
        LSN expected = new LSN(actual.getViewId(), actual.getSequenceNo() + 1L);
        
        // check the LSN of the logEntry to write
        if (lsn.compareTo(actual) <= 0) {
            // entry was already inserted
            rq.free();
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "BASIC: Entry LSN(%s) dropped @LSN(%s).", 
                    lsn.toString(), actual.toString());
            return StageCondition.DEFAULT;
        } else if(!lsn.equals(expected)){
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "BASIC: Entries until LSN(%s) missing @LSN(%s).", 
                    lsn.toString(), actual.toString());
            // we missed one or more entries
            return new StageCondition(lsn);
        } 
        
        // try to finish the request
        try {     
            
            // perform the operation
            babuDB.appendToLocalPersistenceManager(((LogEntry) rq.getArgs()[1]).clone(), 
                    new DatabaseRequestListener<Object>() {
                        
                        @Override
                        public void finished(Object result, Object context) {
                            pacemaker.updateLSN(lsn);
                            updateLastAcknowledgedEntry(lsn);
                            rq.free();
                        }
                        
                        @Override
                        public void failed(BabuDBException error, Object context) {
                            Logging.logMessage(Logging.LEVEL_ALERT, this, 
                                    "Log might be corrupted! A restart may be required!");
                            Logging.logError(Logging.LEVEL_ERROR, this, error);
                            
                            updateLastAsyncInsertedEntry(null);
                            rq.free();
                            // log might get corrupted if this error occurs. a restart may be required.
                        }
                    });
            updateLastAsyncInsertedEntry(lsn);
            
            // replication was successful
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "BASIC: Entry LSN(%s) replicated.", lsn.toString()); 
            return StageCondition.DEFAULT;
        } catch (BabuDBException e) {
            
            // switch to LOAD
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "BASIC: Entry LSN(%s) replication failed: LOAD required.", 
                    lsn.toString());
            return StageCondition.LOAD_LOOPHOLE;           
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.logic.Logic#
     *      run(org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition)
     */
    @Override
    public StageCondition run(StageCondition condition) {
        throw new UnsupportedOperationException();
    }
}