/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * <p>Requests missing {@link LogEntry}s at the master and inserts them into the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class RequestLogic extends Logic {
    
    /** checksum algorithm used to deserialize logEntries */
    private final Checksum checksum = new CRC32();

    /**
     * @param babuDB
     * @param slaveView
     * @param fileIO
     * @param lastOnView
     */
    public RequestLogic(BabuDBInterface babuDB, SlaveView slaveView, FileIOInterface fileIO, 
            AtomicReference<LSN> lastOnView) {
        super(babuDB, slaveView, fileIO, lastOnView);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return LogicID.REQUEST;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.logic.Logic#
     *      run(org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition)
     */
    @Override
    public StageCondition run(StageCondition condition) throws InterruptedException {
        
        assert (condition.logicID == LogicID.REQUEST)  : "PROGRAMATICAL ERROR!";
        
        LSN from = getState();
        LSN until = condition.end;
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "REQUEST: Replica-range is missing: from %s to %s", 
                from.toString(), until.toString());
        
        // get the missing logEntries
        ReusableBuffer[] logEntries = null;
        
        MasterClient master = slaveView.getSynchronizationPartner(until);
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "REQUEST: Replica-Range will be retrieved from %s.", 
                master.getDefaultServerAddress());
        
        try {
            logEntries = master.replica(from, until).get();
            
            // enhancement if the request had detected a master-failover
            if (logEntries.length == 0) {
                LSN lov = babuDB.checkpoint();
                lastOnView.set(lov);
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST: Recognized master failover @ LSN(%s).", 
                        lov.toString());
                return finish(until);
            }
            
            final AtomicInteger count = new AtomicInteger(logEntries.length);
            // insert all logEntries
            LSN check = null;
            for (ReusableBuffer le : logEntries) {
                try {
                    final LogEntry logentry = LogEntry.deserialize(le, checksum);
                    final LSN lsn = logentry.getLSN();
                    
                    // assertion whether the received entry does match the order 
                    // or not
                    assert (check == null || 
                           (check.getViewId() == lsn.getViewId() && 
                            check.getSequenceNo()+1L == lsn.getSequenceNo()) ||
                            check.getViewId()+1 == lsn.getViewId() &&
                            lsn.getSequenceNo() == 1L) : "ERROR: last LSN (" +
                            check.toString() + ") received LSN (" + 
                            lsn.toString() + ")!";
                    check = lsn;
                    
                    // we have to switch the log-file
                    if (lsn.getSequenceNo() == 1L && babuDB.getState().getViewId() < lsn.getViewId()) {
                        lastOnView.set(babuDB.checkpoint());
                    }
                    
                    babuDB.appendToLocalPersistenceManager(logentry, new DatabaseRequestListener<Object>() {
                        
                        @Override
                        public void finished(Object result, Object context) {
                            synchronized (count) {
                                if (count.decrementAndGet() == 0) count.notify();
                            }
                        }
                        
                        @Override
                        public void failed(BabuDBException error, Object context) {
                            Logging.logError(Logging.LEVEL_ERROR, this, error);
                            synchronized (count) {
                                count.set(-1);
                                count.notify();
                            }
                        }
                    });
                } finally {
                    checksum.reset();
                }  
            }
            
            // block until all inserts are finished
            synchronized (count) {
                while (count.get() > 0) count.wait();
            }
            
            // at least one insert failed
            if (count.get() == -1) {
                throw new LogEntryException("At least one insert could not be proceeded.");
            }
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST: Inserted %d logEntries.", logEntries.length); 
            return finish(until);
        } catch (ErrorCodeException ece) {
            if (ece.getCode() == ErrorCode.LOG_UNAVAILABLE) {
                // LOAD
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST: LogFile unavailable @Master(%s).", 
                        master.toString());
                return finish(until, true);
            } else {
                // retry
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST: Transmission to Master (%s) failed.", 
                        master.toString());
                return condition;
            }
        } catch (BabuDBException be) {
            // the insert failed due an DB error -> LOAD
            Logging.logMessage(Logging.LEVEL_WARN, this, "REQUEST: Insert did not succeed, because %s.", be.getMessage());
            Logging.logError(Logging.LEVEL_WARN, this, be);
            return finish(until, true);
        } catch (InterruptedException ie) {
            // crash || shutdown
            throw ie;
        } catch (Exception e) {
            // retry
            Logging.logMessage(Logging.LEVEL_WARN, this, "REQUEST: Retry because of unexpected exception: %s.", e.getMessage());
            Logging.logError(Logging.LEVEL_WARN, this, e);
            return condition;
        } finally {
            if (logEntries != null) {
                for (ReusableBuffer buf : logEntries) {
                    if (buf != null) BufferPool.free(buf);
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.logic.Logic#
     *      run(org.xtreemfs.babudb.replication.service.StageRequest)
     */
    @Override
    public StageCondition run(StageRequest rq) {
        throw new UnsupportedOperationException("PROGRAMATICAL ERROR!");
    }
}