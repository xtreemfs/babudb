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
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseRO;
import org.xtreemfs.include.common.util.FSUtils;

public class SnapshotManager {
    
    public static final String                 SNAP_DIR = "snapshots";
    
    private BabuDB                             dbs;
    
    private Map<String, Map<String, Snapshot>> snapshotDBs;
    
    public SnapshotManager(BabuDB master) throws BabuDBException {
        
        this.dbs = master;
        this.snapshotDBs = Collections.synchronizedMap(new HashMap<String, Map<String, Snapshot>>());
        
        // load persisted snapshots from disk
        for (Entry<String, Database> entry : master.getDatabaseManager().getDatabaseNameMap().entrySet()) {
            
            final File snapDir = new File(master.getConfig().getBaseDir(), entry.getKey() + "/snapshots");
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
    }
    
    /**
     * Returns a read-only database backed by the snapshot with the given unique
     * name.
     * 
     * @param dbName
     *            the name of the database
     * @param snapshotName
     *            the name of the snapshot
     * @return a read-only database backed by the snapshot, or <code>null</code>
     *         if no such snapshot exists
     */
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
    
    /**
     * Triggers the creation of a persistent snapshot of a database. Snapshot
     * properties can be determined in a fine-grained manner, i.e. single key
     * ranges can be selected from single indices.
     * 
     * @param dbName
     * @param snap
     * @throws BabuDBException
     *             if the checkpoint was not successful
     * @throws InterruptedException
     */
    public void createPersistentSnapshot(String dbName, SnapshotConfig snap) throws BabuDBException,
        InterruptedException {
        
        Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        if (snapMap == null) {
            snapMap = new HashMap<String, Snapshot>();
            snapshotDBs.put(dbName, snapMap);
        }
        
        snapMap.put(snap.getName(), new Snapshot(null));
        
        // first, create new in-memory snapshots of all indices
        int[] snapIds = ((DatabaseImpl) dbs.getDatabaseManager().getDatabase(dbName)).createSnapshot(snap
                .getIndices());
        
        // then, enqueue a snapshot materialization request in the
        // checkpointer's queue
        dbs.getCheckpointer().addSnapshotMaterializationRequest(dbName, snapIds, snap, this);
        
        // as long as the snapshot has not been persisted yet, add a view on the
        // current snapshot in the original database to the snapshot DB map
        synchronized (snapshotDBs) {
            Snapshot s = snapMap.get(snap.getName());
            if (s.getView() == null)
                s.setView(new InMemoryView(dbs, dbName, snap.getIndices(), snapIds));
        }
        
    }
    
    public void snapshotComplete(String dbName, SnapshotConfig snap) throws BabuDBException {
        
        // as soon as the snapshot has been completed, replace the entry in the
        // snapshot DB map with a disk index-based BabuDB instance if necessary
        synchronized (snapshotDBs) {
            Snapshot s = snapshotDBs.get(dbName).get(snap.getName());
            s.setView(new DiskIndexView(getSnapshotDir(dbName, snap.getName()), dbs.getDatabaseManager()
                    .getDatabase(dbName).getComparators()));
        }
    }
    
    public void deletePersistentSnapshot(String dbName, String snapshotName) throws BabuDBException {
        
        final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        final Snapshot snap = snapMap.get(snapshotName);
        
        if (snap == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_SNAPSHOT, "snapshot '" + snapshotName
                + "' does not exist");
        
        // shut down and remove the view
        snap.getView().shutdown();
        snapshotDBs.remove(snapshotName);
        
        // delete the snapshot subdirectory on disk
        FSUtils.delTree(new File(getSnapshotDir(dbName, snapshotName)));
    }
    
    public String getSnapshotDir(String dbName, String snapshotName) {
        return dbs.getConfig().getBaseDir() + dbName + "/" + SnapshotManager.SNAP_DIR + "/" + snapshotName;
    }
    
}
