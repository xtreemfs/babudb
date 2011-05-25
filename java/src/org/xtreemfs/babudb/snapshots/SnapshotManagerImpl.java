/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.database.DatabaseRO;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.SnapshotManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.InMemoryProcessing;
import org.xtreemfs.babudb.api.dev.transaction.OperationInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.transaction.Operation;
import org.xtreemfs.babudb.lsmdb.BabuDBTransaction;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

public class SnapshotManagerImpl implements SnapshotManagerInternal {
    
    public static final String                       SNAP_DIR = "snapshots";
    
    private final BabuDBInternal                     dbs;
    
    private final Map<String, Map<String, Snapshot>> snapshotDBs;
    
    public SnapshotManagerImpl(BabuDBInternal dbs) {
        this.dbs = dbs;
        this.snapshotDBs = Collections.synchronizedMap(new HashMap<String, Map<String, Snapshot>>());
        
        initializeTransactionManager();
    }
    
    public void init() throws BabuDBException {
        
        // load persisted snapshots from disk
        for (Entry<String, DatabaseInternal> entry : dbs.getDatabaseManager().getDatabasesInternal()
                .entrySet()) {
            
            final File snapDir = new File(dbs.getConfig().getBaseDir(), entry.getKey() + "/snapshots");
            if (snapDir.exists()) {
                
                Map<String, Snapshot> snapMap = new HashMap<String, Snapshot>();
                snapshotDBs.put(entry.getKey(), snapMap);
                
                boolean compressed = entry.getValue().getLSMDB().getIndex(0).isCompressed();
                boolean mmaped = entry.getValue().getLSMDB().getIndex(0).isMMapEnabled();
                
                String[] snapshots = snapDir.list();
                for (String snapName : snapshots) {
                    BabuDBView view = new DiskIndexView(snapDir + "/" + snapName, entry.getValue()
                            .getComparators(), compressed, mmaped);
                    snapMap.put(snapName, new Snapshot(view));
                }
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.SnapshotManagerInternal#shutdown()
     */
    @Override
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
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.SnapshotManager#createPersistentSnapshot(java.lang.String, org.xtreemfs.babudb.snapshots.SnapshotConfig)
     */
    @Override
    public void createPersistentSnapshot(String dbName, SnapshotConfig snap)
        throws BabuDBException {
        
        // synchronously executing the request
        BabuDBRequestResultImpl<Object> result = new BabuDBRequestResultImpl<Object>();
        dbs.getTransactionManager().makePersistent(
                dbs.getDatabaseManager().createTransaction().createSnapshot(dbName, snap), result);
        result.get();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.SnapshotManagerInternal#snapshotComplete(java.lang.String, 
     *          org.xtreemfs.babudb.snapshots.SnapshotConfig)
     */
    @Override
    public void snapshotComplete(String dbName, SnapshotConfig snap) throws BabuDBException {
        
        // as soon as the snapshot has been completed, replace the entry in the
        // snapshot DB map with a disk index-based BabuDB instance if necessary
        synchronized (snapshotDBs) {
        	
        	DatabaseInternal db = dbs.getDatabaseManager().getDatabase(dbName);
        	boolean compressed = db.getLSMDB().getIndex(0).isCompressed();
        	boolean mmaped = db.getLSMDB().getIndex(0).isMMapEnabled();
        	
            Snapshot s = snapshotDBs.get(dbName).get(snap.getName());
            s.setView(new DiskIndexView(getSnapshotDir(dbName, snap.getName()), dbs.getDatabaseManager()
                    .getDatabase(dbName).getComparators(), compressed, mmaped));
        }
    }
    
    @Override
    public void deletePersistentSnapshot(String dbName, String snapshotName) throws BabuDBException {
        
        BabuDBRequestResultImpl<Object> result = new BabuDBRequestResultImpl<Object>();
        dbs.getTransactionManager().makePersistent(
                dbs.getDatabaseManager().createTransaction().deleteSnapshot(
                        dbName, snapshotName), result);
        result.get();
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
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.SnapshotManagerInternal#deleteAllSnapshots(java.lang.String)
     */
    @Override
    public void deleteAllSnapshots(String dbName) throws BabuDBException {
        
        final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
        if (snapMap != null) {
            
            for (Entry<String, Snapshot> snap : snapMap.entrySet()) {
                
                // shut down the view
                snap.getValue().shutdown();
                
                // if a snapshot materialization request is currently in the
                // checkpointer queue, remove it
                dbs.getCheckpointer().removeSnapshotMaterializationRequest(dbName, snap.getKey());
                
            }
            
            // remove the map entry
            snapshotDBs.remove(dbName);
        }
        FSUtils.delTree(new File(getSnapshotDir(dbName, null)));
        
        // no delete log entries for the snapshots are needed here, since the
        // method will only be invoked when the database itself is deleted
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.SnapshotManagerInternal#getSnapshotDir(java.lang.String, 
     *          java.lang.String)
     */
    @Override
    public String getSnapshotDir(String dbName, String snapshotName) {
        return dbs.getConfig().getBaseDir() + dbName + "/" + SnapshotManagerImpl.SNAP_DIR + "/"
            + (snapshotName == null ? "" : snapshotName);
    }
    
    /**
     * Feed the transactionManager with the knowledge on how to handle snapshot related requests.
     */
    private void initializeTransactionManager() {
        
        dbs.getTransactionManager().registerInMemoryProcessing(Operation.TYPE_CREATE_SNAP, 
                new InMemoryProcessing() {
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                ObjectInputStream oin = null;
                try {
                    oin = new ObjectInputStream(new ByteArrayInputStream(serialized.array()));
                    int dbId = oin.readInt();
                    SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                    
                    return new Object[] { dbId, snap };
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.IO_ERROR,
                            "Could not deserialize operation of type " + Operation.TYPE_CREATE_SNAP + 
                                ", because: "+e.getMessage(), e);
                } finally {
                    try {
                        serialized.flip();
                        if (oin != null) oin.close();
                    } catch (IOException ioe) {
                        /* who cares? */
                    }
                }
            }
            
            @Override
            public OperationInternal convertToOperation(Object[] args) {
                return new BabuDBTransaction.BabuDBOperation(Operation.TYPE_CREATE_SNAP, (String) null, 
                        new Object[] { args[0], args[1] });
            }
            
            @Override
            public Object process(OperationInternal operation) throws BabuDBException {
                
                Object[] args = operation.getParams();
                
                // parse args
                int dbId = (Integer) args[0];
                SnapshotConfig snap = (SnapshotConfig) args[1];
                
                // complete arguments
                if (dbId == InsertRecordGroup.DB_ID_UNKNOWN && 
                    operation.getDatabaseName() != null) {
                    dbId = dbs.getDatabaseManager().getDatabase(
                            operation.getDatabaseName()).getLSMDB().getDatabaseId();
                    operation.updateParams(new Object[] { dbId, snap });                  
                } else if (operation.getDatabaseName() == null) {
                    operation.updateDatabaseName(
                            dbs.getDatabaseManager().getDatabase(dbId).getName());
                }
                
                Map<String, Snapshot> snapMap = snapshotDBs.get(operation.getDatabaseName());
                if (snapMap == null) {
                    snapMap = new HashMap<String, Snapshot>();
                    snapshotDBs.put(operation.getDatabaseName(), snapMap);
                }
                
                // if the snapshot already exists ...
                if (snapMap.containsKey(snap.getName())) {
                    
                    throw new BabuDBException(ErrorCode.SNAP_EXISTS, "snapshot '" + snap.getName()
                        + "' already exists");
                }
                
                snapMap.put(snap.getName(), new Snapshot(null));
                
                // first, create new in-memory snapshots of all indices
                int[] snapIds = null;
                try {
                    dbs.getTransactionManager().lockService();
                    
                    // create the snapshot
                    snapIds = dbs.getDatabaseManager().getDatabase(dbId).getLSMDB().createSnapshot(
                            snap.getIndices());
                } catch (InterruptedException e) {
                    throw new BabuDBException(ErrorCode.INTERRUPTED, e.getMessage());
                } finally {
                    dbs.getTransactionManager().unlockService();
                }
                
                // then, enqueue a snapshot materialization request in the
                // checkpointer's queue
                dbs.getCheckpointer().addSnapshotMaterializationRequest(operation.getDatabaseName(), 
                        snapIds, snap);
                
                // as long as the snapshot has not been persisted yet, add a view on
                // the current snapshot in the original database to the snapshot DB map
                synchronized (snapshotDBs) {
                    
                    Snapshot s = snapMap.get(snap.getName());
                    if (s.getView() == null) {
                        s.setView(new InMemoryView(dbs, operation.getDatabaseName(), snap, 
                                snapIds));
                    }
                }
                
                return null;
            }
        });

        dbs.getTransactionManager().registerInMemoryProcessing(Operation.TYPE_DELETE_SNAP, 
                new InMemoryProcessing() {
                        
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                byte[] payload = serialized.array();
                int offs = payload[0];
                String dbName = new String(payload, 1, offs);
                String snapName = new String(payload, offs + 1, payload.length - offs - 1);
                serialized.flip();
                
                return new Object[] { dbName, snapName };
            }
            
            @Override
            public OperationInternal convertToOperation(Object[] args) {
                return new BabuDBTransaction.BabuDBOperation(Operation.TYPE_DELETE_SNAP, (String) args[0], 
                        new Object[] { args[1] });
            }

            @Override
            public Object process(OperationInternal operation) throws BabuDBException {

                // parse args
                String snapshotName = (String) operation.getParams()[0];
                
                final Map<String, Snapshot> snapMap = snapshotDBs.get(operation.getDatabaseName());
                if(snapMap == null) {
                    throw new BabuDBException(ErrorCode.NO_SUCH_SNAPSHOT, 
                            "snapshot '" + snapshotName + 
                            "' does not exist"); 
                }
                
                final Snapshot snap = snapMap.get(snapshotName);
                
                // if the snapshot does not exist ...
                if (snap == null) {
                    throw new BabuDBException(ErrorCode.NO_SUCH_SNAPSHOT, 
                            "snapshot '" + snapshotName + 
                            "' does not exist");
                }
                
                // shut down and remove the view
                snap.getView().shutdown();
                snapMap.remove(snapshotName);
                
                // if a snapshot materialization request is currently in the
                // checkpointer queue, remove it
                dbs.getCheckpointer().removeSnapshotMaterializationRequest(
                        operation.getDatabaseName(), snapshotName);
                
                // delete the snapshot subdirectory on disk if available
                FSUtils.delTree(new File(getSnapshotDir(operation.getDatabaseName(), 
                        snapshotName)));
                
                return null;
            }
        });
    }
}
