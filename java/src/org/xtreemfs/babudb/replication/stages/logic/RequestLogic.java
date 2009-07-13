/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.stages.ReplicationStage;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;

/**
 * <p>Requests missing {@link LogEntry}s at the 
 * master and inserts them into the DBS.</p>
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class RequestLogic extends Logic {

    public RequestLogic(ReplicationStage stage) {
        super(stage);
    }

    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.stages.logic.Logic#getId()
     */
    @Override
    public LogicID getId() {
        return LogicID.REQUEST;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        LSNRange missing = stage.getMissing();
        
        // get the missing logEntries
        RPCResponse<LogEntries> rp = stage.dispatcher.master.getReplica(missing);
        try {
            LogEntries logEntries = (LogEntries) rp.get();
            final AtomicInteger count = new AtomicInteger(logEntries.size());
            
            // insert all logEntries
            for (org.xtreemfs.babudb.interfaces.LogEntry le : logEntries) {
                LSMDBRequest dbRq = SharedLogic.retrieveRequest(le.getPayload(), new SimplifiedBabuDBRequestListener() {
                
                    @Override
                    void finished(Object context, BabuDBException error) {
                        synchronized (count) {
                            // insert succeeded
                            if (error == null && count.decrementAndGet() == 0) count.notify();
                            // insert failed
                            else {
                                count.set(-1);
                                count.notify();
                            }
                        }
                    }
                }, null, stage.dispatcher.db.databases);
                SharedLogic.writeLogEntry(dbRq, stage.dispatcher.db);
            }
            
            // wait until all inserts are finished
            synchronized (count) {
                count.wait();
            }
            
            // at least one insert failed
            if (count.get() == -1) throw new Exception();
            
            stage.dispatcher.updateLatestLSN(
                    new LSN(missing.getViewId(),missing.getSequenceEnd()));
            stage.setMissing(null);
            stage.setCurrentLogic(LogicID.BASIC);
        } catch (Exception e) {
            stage.setCurrentLogic(LogicID.LOAD);
        }
    }
}