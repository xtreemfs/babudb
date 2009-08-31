/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages.logic;

import static org.xtreemfs.babudb.log.LogEntry.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.SimplifiedBabuDBRequestListener;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;

/**
 * Static functions used in more than one {@link Logic}.
 * 
 * @author flangner
 * @since 06/08/2009
 */

final class SharedLogic {
        
    /**
     * <p>
     * Retrieve the information from a {@link LogEntry} to replicate it's operation
     * to the {@link BabuDB}.
     * !!!The entry will not be freed here!!!
     * </p>
     * 
     * @param entry - the {@link LogEntry}.
     * @param listener - will be notified, if insert was replicated.
     * @param sListener - will be notified, if create/copy/delete/snap was replicated. 
     *                                      
     * @param dbs - the {@link BabuDB} database system.
     * @throws BabuDBException 
     * @throws InterruptedException 
     * @throws IOException 
     */
    static void handleLogEntry(LogEntry entry, SimplifiedBabuDBRequestListener listener, SyncListener sListener, BabuDB dbs) throws BabuDBException, InterruptedException, IOException {
        // perform the operation
        if (entry.getPayloadType() == PAYLOAD_TYPE_INSERT) {
            // prepare the LSMDBrequest
            LSMDBRequest rq = SharedLogic.retrieveRequest(entry, listener, 
                    ((DatabaseManagerImpl) dbs.getDatabaseManager()).getDatabasesById());
            
            // start the LSMDBrequest
            SharedLogic.writeLogEntry(rq, dbs);
        } else {          
            // check the payload type
            switch (entry.getPayloadType()) {                      
            case PAYLOAD_TYPE_CREATE:
                // deserialize the create call
                int indices = entry.getPayload().getInt();
                String dbName = entry.getPayload().getString();
                ((DatabaseManagerImpl) dbs.getDatabaseManager()).
                    proceedCreate(dbName, indices, null);
                break;
                
            case PAYLOAD_TYPE_COPY:
                // deserialize the copy call
                String dbSource = entry.getPayload().getString();
                dbName = entry.getPayload().getString();
                ((DatabaseManagerImpl) dbs.getDatabaseManager()).
                    proceedCopy(dbSource, dbName);
                break;
                
            case PAYLOAD_TYPE_DELETE:
                // deserialize the create operation call
                dbName = entry.getPayload().getString();
                ((DatabaseManagerImpl) dbs.getDatabaseManager()).
                    proceedDelete(dbName);
                break;
                
            case PAYLOAD_TYPE_SNAP:
                ObjectInputStream oin = null;
                try {
                    oin = new ObjectInputStream(new ByteArrayInputStream(entry.getPayload().
                            array()));
                    // deserialize the snapshot configuration
                    int dbId = oin.readInt();
                    SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                    ((SnapshotManagerImpl) dbs.getSnapshotManager()).
                        createPersistentSnapshot(((DatabaseManagerImpl) dbs.
                                getDatabaseManager()).getDatabase(dbId).getName(), 
                                snap, false);
                } catch (Exception e) {
                    throw new IOException("Could not deserialize operation of type "+
                            entry.getPayloadType()+", because: "+e.getMessage(), e);
                } finally {
                    if (oin != null) oin.close();
                }
                break;
                    
            default: new IOException("unknown payload-type");
            }
            entry.getPayload().flip();
            entry.setListener(sListener);
            
            // append logEntry to the logFile
            dbs.getLogger().append(entry);  
        }
    }
    
/*
 * private methods
 */
    
    /**
     * @param context
     * @param listener
     * @param le - {@link LogEntry}
     * @param dbs 
     * @return the LSMDBRequest retrieved from the logEntry
     * @throws BabuDBException 
     *              if the DBS is not consistent and should be loaded.
     */
    private static LSMDBRequest retrieveRequest(LogEntry le, SimplifiedBabuDBRequestListener listener, Map<Integer, Database> dbs) throws BabuDBException {       
        // build a LSMDBRequest
        InsertRecordGroup irg = InsertRecordGroup.deserialize(le.getPayload());

        if (!dbs.containsKey(irg.getDatabaseId()))
            throw new BabuDBException(ErrorCode.NO_SUCH_DB,"Database does not exist.Load DB!");

        return new LSMDBRequest(((DatabaseImpl) dbs.get(irg.getDatabaseId())).getLSMDB(), 
                listener, irg, null);
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
    private static void writeLogEntry(LSMDBRequest rq, BabuDB dbs) throws InterruptedException {
        int dbId = rq.getInsertData().getDatabaseId();
        LSMDBWorker w = dbs.getWorker(dbId);
        w.addRequest(rq);
    }
}