/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.interfaces.utils.ONCRPCException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.babudb.replication.stages.ReplicationStage.ConnectionLostException;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;

import static org.xtreemfs.babudb.replication.stages.ReplicationStage.ConnectionLostException;
import static org.xtreemfs.babudb.replication.stages.logic.LogicID.*;

/**
 * <p>Requests missing {@link LogEntry}s at the 
 * master and inserts them into the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class RequestLogic extends Logic {

    private static final Checksum checksum = new CRC32();
    
    public RequestLogic(ReplicationStage stage) {
        super(stage);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return REQUEST;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() throws InterruptedException, ConnectionLostException{
        // get and reset the missing range
        LSNRange missing = stage.missing;
        stage.missing = null;
        Logging.logMessage(Logging.LEVEL_INFO, this, "Replica-range is missing: %s", missing.toString());
        
        // get the missing logEntries
        RPCResponse<LogEntries> rp = stage.dispatcher.master.getReplica(missing);
        LogEntries logEntries = null;
        try {
            logEntries = (LogEntries) rp.get();
            final AtomicInteger count = new AtomicInteger(logEntries.size());
            // insert all logEntries
            for (org.xtreemfs.babudb.interfaces.LogEntry le : logEntries) {
                LogEntry logentry = null;
                try {
                    logentry = LogEntry.deserialize(le.getPayload(), checksum);
                    LSMDBRequest dbRq = SharedLogic.retrieveRequest(logentry, new SimplifiedBabuDBRequestListener() {
                    
                        @Override
                        void finished(Object context, BabuDBException error) {
                            synchronized (count) {
                                // insert succeeded
                                if (error == null) {
                                    assert (context instanceof LSN);
                                    stage.loadUntil = (LSN) context;
                                    if (count.decrementAndGet() == 0)
                                        count.notify();
                                // insert failed
                                } else if (error != null){
                                    Logging.logError(Logging.LEVEL_ERROR, this, error);
                                    count.set(-1);
                                    count.notify();
                                }
                            }
                        }
                    }, logentry.getLSN(), ((DatabaseManagerImpl) stage.dispatcher.dbs.getDatabaseManager()).getDatabaseMap());
                    SharedLogic.writeLogEntry(dbRq, stage.dispatcher.dbs);
                } finally {
                    checksum.reset();
                    if (logentry!=null) logentry.free();
                }  
            }
            
            // block until all inserts are finished
            synchronized (count) {
                while (count.get() > 0)
                    count.wait();
            }
            
            // at least one insert failed
            if (count.get() == -1) throw new LogEntryException("At least one insert could not be proceeded.");
 
            // all went fine --> back to basic
            stage.dispatcher.updateLatestLSN(stage.loadUntil);
            stage.setLogic(BASIC);
            
        } catch (ONCRPCException once) {
            // remote failure (connection lost)
            throw new ConnectionLostException(once.getMessage());
        } catch (IOException ioe) {
            // system failure on receiving the transmission
            Logging.logError(Logging.LEVEL_WARN, this, ioe);
            stage.setLogic(LOAD);
        } catch (LogEntryException lee) {
            // decoding failed --> retry with new range
            Logging.logError(Logging.LEVEL_WARN, this, lee);
            stage.missing = new LSNRange(missing.getViewId(), stage.loadUntil.getSequenceNo()+1L, missing.getSequenceEnd());
        } catch (BabuDBException be) {
            // the insert failed due an DB error
            Logging.logError(Logging.LEVEL_WARN, this, be);
            stage.setLogic(LOAD);
        } finally {
            if (rp!=null) rp.freeBuffers();
            if (logEntries!=null) 
                for (org.xtreemfs.babudb.interfaces.LogEntry le : logEntries) 
                    if (le.getPayload()!=null) BufferPool.free(le.getPayload());
        }
    }
}