/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;

/**
 * Static functions used in more than one {@link Logic}.
 * 
 * @author flangner
 * @since 06/08/2009
 */

final class SharedLogic {
    
    /**
     * @param context
     * @param listener
     * @param le - {@link LogEntry}
     * @param dbs 
     * @return the LSMDBRequest retrieved from the logEntry
     * @throws BabuDBException 
     *              if the DBS is not consistent and should be loaded.
     */
    static LSMDBRequest retrieveRequest(LogEntry le, BabuDBRequestListener listener, Object context, Map<Integer, Database> dbs) throws BabuDBException {       
        // build a LSMDBRequest
        InsertRecordGroup irg = InsertRecordGroup.deserialize(le.getPayload());

        if (!dbs.containsKey(irg.getDatabaseId()))
            throw new BabuDBException(ErrorCode.NO_SUCH_DB,"Database does not exist.Load DB!");

        return new LSMDBRequest(((DatabaseImpl) dbs.get(irg.getDatabaseId())).getLSMDB(), 
                listener, irg, context);
    }
    
    /**
     * <p>
     * Write the {@link LogEntry} given by a generated {@link LSMDBRequest}
     * <code>rq</code> the DiskLogger and insert it into the LSM-tree.
     * </p>
     * 
     * @param dbs
     * @param rq
     * @throws InterruptedException
     *             if an error occurs.
     */
    static void writeLogEntry(LSMDBRequest rq, BabuDB dbs) throws InterruptedException {
        int dbId = rq.getInsertData().getDatabaseId();
        LSMDBWorker w = dbs.getWorker(dbId);
        w.addRequest(rq);
    }
}