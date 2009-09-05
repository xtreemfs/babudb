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

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
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
     * @param listener - will be notified, if entry was replicated. 
     *                                      
     * @param dbs - the {@link BabuDB} database system.
     * @throws BabuDBException 
     * @throws InterruptedException the entry will not be written, if thrown.
     */
    static void handleLogEntry(LogEntry entry, SyncListener listener, BabuDB dbs) 
            throws BabuDBException, InterruptedException {
        DatabaseManagerImpl dbMan = (DatabaseManagerImpl) dbs.getDatabaseManager();

        // check the payload type
        switch (entry.getPayloadType()) { 
        
        case PAYLOAD_TYPE_INSERT:
            InsertRecordGroup irg = InsertRecordGroup.deserialize(entry.getPayload());
            dbMan.insert(irg);
            break;
        
        case PAYLOAD_TYPE_CREATE:
            // deserialize the create call
            int dbId = entry.getPayload().getInt();
            String dbName = entry.getPayload().getString();
            int indices = entry.getPayload().getInt();
            if (dbMan.getDatabase(dbId) == null)
                dbMan.proceedCreate(dbName, indices, null);
            break;
            
        case PAYLOAD_TYPE_COPY:
            // deserialize the copy call
            entry.getPayload().getInt(); // do not delete!
            dbId = entry.getPayload().getInt();
            String dbSource = entry.getPayload().getString();
            dbName = entry.getPayload().getString();
            if (dbMan.getDatabase(dbId) == null)
                dbMan.proceedCopy(dbSource, dbName);
            break;
            
        case PAYLOAD_TYPE_DELETE:
            // deserialize the create operation call
            dbId = entry.getPayload().getInt();
            dbName = entry.getPayload().getString();
            if (dbMan.getDatabase(dbId) != null)
                dbMan.proceedDelete(dbName);
            break;
            
        case PAYLOAD_TYPE_SNAP:
            ObjectInputStream oin = null;
            try {
                oin = new ObjectInputStream(new ByteArrayInputStream(
                        entry.getPayload().array()));
                // deserialize the snapshot configuration
                dbId = oin.readInt();
                SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                ((SnapshotManagerImpl) dbs.getSnapshotManager()).
                    createPersistentSnapshot(dbMan.getDatabase(dbId).
                            getName(),snap, false);
            } catch (Exception e) {
                throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,
                        "Could not deserialize operation of type "+entry.
                        getPayloadType()+", because: "+e.getMessage(), e);
            } finally {
                try {
                if (oin != null) oin.close();
                } catch (IOException ioe) {
                    /* who cares? */
                }
            }
            break;
                
        default: new BabuDBException(ErrorCode.INTERNAL_ERROR,
                "unknown payload-type");
        }
        entry.getPayload().flip();
        entry.setListener(listener);
        
        // append logEntry to the logFile
        dbs.getLogger().append(entry);  
    }
}