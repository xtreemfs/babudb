/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * Static functions used in more than one {@link Logic}.
 * 
 * @author flangner
 * @since 06/08/2009
 */

final class SharedLogic {

    private final static Checksum checksum = new CRC32();
    
    /**
     * @param context
     * @param listener
     * @param le - LogEntry
     * @param dbs 
     * @return the LSMDBRequest retrieved from the logEntry
     * @throws IOException
     *             if the DB is not consistent and should be loaded.
     */
    static LSMDBRequest retrieveRequest(LogEntry le, BabuDBRequestListener listener, Object context, Map<Integer, Database> dbs) throws IOException {       
        // build a LSMDBRequest
        ReusableBuffer buf = le.getPayload().createViewBuffer();
        InsertRecordGroup irg = InsertRecordGroup.deserialize(buf);
        BufferPool.free(buf);

        if (!dbs.containsKey(irg.getDatabaseId()))
            throw new IOException("Database does not exist.Load DB!");

        return new LSMDBRequest(((DatabaseImpl) dbs.get(irg.getDatabaseId())).getLSMDB(), 
                listener, irg, context);
    }
    
    /**
     * @param context
     * @param listener
     * @param buffer
     * @param dbs 
     * @return the LSMDBRequest retrieved from the buffer
     * @throws IOException
     *             if the DB is not consistent and should be loaded.
     */
    static LSMDBRequest retrieveRequest(ReusableBuffer buffer, BabuDBRequestListener listener, Object context, Map<Integer, Database> dbs) throws IOException {
        // parse logEntry
        LogEntry le = null;
        try {
            le = LogEntry.deserialize(buffer, checksum);
        } catch (LogEntryException e) {
            throw new IOException(e.getMessage());
        } finally {
            checksum.reset();
        }        
        return retrieveRequest(le, listener, context, dbs);
    }
    
    /**
     * <p>
     * Write the {@link LogEntry} given by a generated {@link LSMDBRequest}
     * <code>rq</code> the DiskLogger and insert it into the LSM-tree.
     * </p>
     * 
     * @param db
     * @param rq
     * @throws InterruptedException
     *             if an error occurs.
     */
    static void writeLogEntry(LSMDBRequest rq, BabuDB db) throws InterruptedException {
        int dbId = rq.getInsertData().getDatabaseId();
        LSMDBWorker w = db.getWorker(dbId);
        w.addRequest(rq);
    }
}