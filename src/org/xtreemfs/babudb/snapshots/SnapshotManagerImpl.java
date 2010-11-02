/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.api.Database;
import org.xtreemfs.babudb.api.SnapshotManager;
import org.xtreemfs.babudb.api.exceptions.BabuDBException;
import org.xtreemfs.babudb.api.exceptions.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseRO;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

public class SnapshotManagerImpl implements SnapshotManager {
    
    public static final String                       SNAP_DIR = "snapshots";
    
    private final BabuDBImpl                         dbs;
    
    private final Map<String, Map<String, Snapshot>> snapshotDBs;
    
    public SnapshotManagerImpl(BabuDBImpl dbs) {
        this.dbs = dbs;
        this.snapshotDBs = Collections.synchronizedMap(new HashMap<String, Map<String, Snapshot>>());
    }
    
    public void init() throws BabuDBException {
        
        // load persisted snapshots from disk
        for (Entry<String, Database> entry : ((DatabaseManagerImpl) dbs.getDatabaseManager()).getDatabases()
                .entrySet()) {
            
            final File snapDir = new File(dbs.getConfig().getBaseDir(), entry.getKey() + "/snapshots");
            if (snapDir.exists()) {
                
                Map<String, Snapshot> snapMap = new HashMap<String, Snapshot>();
                snapshotDBs.put(entry.getKey(), snapMap);
                
                String[] snapshots = snapDir.list();
                for (String snapName : snapshots) {
                    BabuDBView view = new DiskIndexView(snapDir + "/" + snapName, entry.getValue()
                            .getComparators());
                    snapMap.put(snapName, new Snapshot(view));
                }
            }
        }
    }
    
    public void shutdown() throws BabuDBException {
        for (Map<String, Snapshot> snapshots : snapshotDBs.values())
            for (Snapshot snapshot : snapshots.values())
                snapshot.shutdown();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "snapshot manager shut down successfully");
    }
    
    @Override
    public DatabaseRO getSnapshotDB(String dbName, String snapshotName) throws BabuDBException {
        
        Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        if (snapMap == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_SNAPSHOT, "no snapshots exist for database '"
                + dbName + "'");
        
        Snapshot snap = snapMap.get(snapshotName);
        if (snap == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_SNAPSHOT, "no snapshot '" + snapshotName
                + "' exists for database '" + dbName + "'");
        
        return snap;
    }
    
    @Override
    public void createPersistentSnapshot(String dbName, SnapshotConfig snap) throws BabuDBException {
        createPersistentSnapshot(dbName, snap, true);
    }
    
    /**
     * Triggers the creation of a persistent snapshot of a database. Snapshot
     * properties can be determined in a fine-grained manner, i.e. single key
     * ranges can be selected from single indices.
     * 
     * @param dbName
     *            the name of the database to create the snapshot from
     * @param snap
     *            the snapshot configuration
     * @throws BabuDBException
     *             if snapshot creation failed
     */
    public void createPersistentSnapshot(String dbName, SnapshotConfig snap, boolean createLogEntry)
        throws BabuDBException {
        
        try {
            Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
            if (snapMap == null) {
                snapMap = new HashMap<String, Snapshot>();
                snapshotDBs.put(dbName, snapMap);
            }
            
            // if the snapshot exists already ...
            if (snapMap.containsKey(snap.getName())) {
                
                // if no log entry needs to be created, i.e. the log is being
                // replayed, ignore the request
                if (!createLogEntry)
                    return;
                
                throw new BabuDBException(ErrorCode.SNAP_EXISTS, "snapshot '" + snap.getName()
                    + "' already exists");
            }
            
            snapMap.put(snap.getName(), new Snapshot(null));
            
            // first, create new in-memory snapshots of all indices
            int[] snapIds = ((DatabaseImpl) dbs.getDatabaseManager().getDatabase(dbName)).createSnapshot(
                snap, createLogEntry);
            
            // then, enqueue a snapshot materialization request in the
            // checkpointer's queue
            ((CheckpointerImpl) dbs.getCheckpointer()).addSnapshotMaterializationRequest(dbName, snapIds,
                snap);
            
            // as long as the snapshot has not been persisted yet, add a view on
            // the
            // current snapshot in the original database to the snapshot DB map
            synchronized (snapshotDBs) {
                Snapshot s = snapMap.get(snap.getName());
                if (s.getView() == null)
                    s.setView(new InMemoryView(dbs, dbName, snap, snapIds));
            }
            
        } catch (InterruptedException exc) {
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "an error occurred", exc);
        }
        
    }
    
    /**
     * inovked by the framework when snapshot creation has completed
     */
    public void snapshotComplete(String dbName, SnapshotConfig snap) throws BabuDBException {
        
        // as soon as the snapshot has been completed, replace the entry in the
        // snapshot DB map with a disk index-based BabuDB instance if necessary
        synchronized (snapshotDBs) {
            Snapshot s = snapshotDBs.get(dbName).get(snap.getName());
            s.setView(new DiskIndexView(getSnapshotDir(dbName, snap.getName()), dbs.getDatabaseManager()
                    .getDatabase(dbName).getComparators()));
        }
    }
    
    @Override
    public void deletePersistentSnapshot(String dbName, String snapshotName) throws BabuDBException {
        deletePersistentSnapshot(dbName, snapshotName, true);
    }
    
    public void deletePersistentSnapshot(String dbName, String snapshotName, boolean createLogEntry)
        throws BabuDBException {
        
        final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        if(snapMap == null)
            return;
        
        final Snapshot snap = snapMap.get(snapshotName);
        
        // if the snapshot does not exist ...
        if (snap == null) {
            
            // if no log entry needs to be created, i.e. the log is being
            // replayed, ignore the request
            if (!createLogEntry)
                return;
            
            throw new BabuDBException(ErrorCode.NO_SUCH_SNAPSHOT, "snapshot '" + snapshotName
                + "' does not exist");
        }
        
        // shut down and remove the view
        snap.getView().shutdown();
        snapMap.remove(snapshotName);
        
        // if a snapshot materialization request is currently in the
        // checkpointer queue, remove it
        ((CheckpointerImpl) dbs.getCheckpointer()).removeSnapshotMaterializationRequest(dbName, snapshotName);
        
        // delete the snapshot subdirectory on disk if available
        FSUtils.delTree(new File(getSnapshotDir(dbName, snapshotName)));
        
        // if required, add deletion request to log
        if (createLogEntry) {
            
            byte[] data = new byte[1 + dbName.length() + snapshotName.length()];
            byte[] dbNameBytes = dbName.getBytes();
            byte[] snapNameBytes = snapshotName.getBytes();
            
            assert (dbName.length() <= Byte.MAX_VALUE);
            data[0] = (byte) dbName.length();
            System.arraycopy(dbNameBytes, 0, data, 1, dbNameBytes.length);
            System.arraycopy(snapNameBytes, 0, data, 1 + dbNameBytes.length, snapNameBytes.length);
            
            ReusableBuffer buf = ReusableBuffer.wrap(data);
            DatabaseManagerImpl.metaInsert(LogEntry.PAYLOAD_TYPE_SNAP_DELETE, buf, dbs.getLogger());
        }
    }
    
    @Override
    public String[] getAllSnapshots(String dbName) {
        
        final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        if (snapMap != null) {
            Set<String> names = snapMap.keySet();
            return names.toArray(new String[names.size()]);
        }

        else
            return new String[0];
    }
    
    public void deleteAllSnapshots(String dbName) throws BabuDBException {
        
        final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        if (snapMap != null) {
            
            for (Entry<String, Snapshot> snap : snapMap.entrySet()) {
                
                // shut down the view
                snap.getValue().shutdown();
                
                // if a snapshot materialization request is currently in the
                // checkpointer queue, remove it
                ((CheckpointerImpl) dbs.getCheckpointer()).removeSnapshotMaterializationRequest(dbName, snap
                        .getKey());
                
            }
            
            // remove the map entry
            snapshotDBs.remove(dbName);
        }
        FSUtils.delTree(new File(getSnapshotDir(dbName, null)));
        
        // no delete log entries for the snapshots are needed here, since the
        // method will only be invoked when the database itself is deleted
    }
    
    public String getSnapshotDir(String dbName, String snapshotName) {
        return dbs.getConfig().getBaseDir() + dbName + "/" + SnapshotManagerImpl.SNAP_DIR + "/"
            + (snapshotName == null ? "" : snapshotName);
    }
    
}
