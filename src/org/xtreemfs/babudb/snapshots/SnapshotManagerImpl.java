/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.snapshots;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.DatabaseRO;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.InMemoryProcessing;
import org.xtreemfs.babudb.api.dev.SnapshotManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_SNAP;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_SNAP_DELETE;

public class SnapshotManagerImpl implements SnapshotManagerInternal {
    
    public static final String                       SNAP_DIR = "snapshots";
    
    private final BabuDBInternal                     dbs;
    
    private final Map<String, Map<String, Snapshot>> snapshotDBs;
    
    public SnapshotManagerImpl(BabuDBInternal dbs) {
        this.dbs = dbs;
        this.snapshotDBs = Collections.synchronizedMap(new HashMap<String, Map<String, Snapshot>>());
        
        initializePerisistenceManager();
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
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_SNAP, 
                new Object[]{ dbs.getDatabaseManager().getDatabase(dbName).getLSMDB()
                                 .getDatabaseId(), snap }).get();
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
        deletePersistentSnapshot(dbName, snapshotName, true);
    }
    
    public void deletePersistentSnapshot(String dbName, String snapshotName, boolean createLogEntry)
            throws BabuDBException {
                
        // if required, add deletion request to log
        if (createLogEntry) {            
            dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_SNAP_DELETE, 
                    new Object[] { dbName, snapshotName }).get();
        } else {
            
            final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
            if(snapMap == null)
                return;
            
            final Snapshot snap = snapMap.get(snapshotName);
            
            // if the snapshot does not exist ...
            if (snap == null) {
                
                // if no log entry needs to be created, i.e. the log is being
                // replayed, ignore the request
                return;
            }
            
            // shut down and remove the view
            snap.getView().shutdown();
            snapMap.remove(snapshotName);
            
            // if a snapshot materialization request is currently in the
            // checkpointer queue, remove it
            dbs.getCheckpointer().removeSnapshotMaterializationRequest(dbName, snapshotName);
            
            // delete the snapshot sub-directory on disk if available
            FSUtils.delTree(new File(getSnapshotDir(dbName, snapshotName)));
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
     * Feed the persistenceManager with the knowledge on how to handle snapshot related requests.
     */
    private void initializePerisistenceManager() {
        
        dbs.getPersistenceManager().registerInMemoryProcessing(PAYLOAD_TYPE_SNAP, 
                new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest(Object[] args) throws BabuDBException {
                
                // parse args
                int dbId = (Integer) args[0];
                SnapshotConfig snap = (SnapshotConfig) args[1];
                
                // serialize the snapshot configuration
                ReusableBuffer buf = null;
                try {
                    ByteArrayOutputStream bout = new ByteArrayOutputStream();
                    ObjectOutputStream oout = new ObjectOutputStream(bout);
                    oout.writeInt(dbId);
                    // TODO add dbName to simplify the in-memory processing
                    oout.writeObject(snap);
                    buf = ReusableBuffer.wrap(bout.toByteArray());
                    oout.close();
                } catch (IOException exc) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "could not serialize snapshot " +
                                "configuration: " + snap.getClass(), exc);
                }
                
                return buf;
            }

            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                ObjectInputStream oin = null;
                try {
                    oin = new ObjectInputStream(new ByteArrayInputStream(serialized.array()));
                    int dbId = oin.readInt();
                    // TODO add dbName to simplify the in-memory processing
                    SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                    
                    return new Object[] { dbId, snap };
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.IO_ERROR,
                            "Could not deserialize operation of type " + PAYLOAD_TYPE_SNAP + 
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
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#before(java.lang.Object[])
             */
            @Override
            public void before(Object[] args) throws BabuDBException {
                
                // parse args
                int dbId = (Integer) args[0];
                SnapshotConfig snap = (SnapshotConfig) args[1];
                // TODO add dbName to simplify the in-memory processing
                String dbName = dbs.getDatabaseManager().getDatabase(dbId).getName();
                
                Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
                if (snapMap == null) {
                    snapMap = new HashMap<String, Snapshot>();
                    snapshotDBs.put(dbName, snapMap);
                }
                
                // if the snapshot exists already ...
                if (snapMap.containsKey(snap.getName())) {
                    
                    throw new BabuDBException(ErrorCode.SNAP_EXISTS, "snapshot '" + snap.getName()
                        + "' already exists");
                }
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#after(java.lang.Object[])
             */
            @Override
            public void after(Object[] args) throws BabuDBException {
                
                // parse args
                int dbId = (Integer) args[0];
                SnapshotConfig snap = (SnapshotConfig) args[1];
                // TODO add dbName to simplify the in-memory processing
                DatabaseInternal org = dbs.getDatabaseManager().getDatabase(dbId);
                
                String dbName = org.getName();
                
                Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
                snapMap.put(snap.getName(), new Snapshot(null));
                
                // first, create new in-memory snapshots of all indices
                int[] snapIds = null;
                try {
                    dbs.getPersistenceManager().lockService();
                    
                    // create the snapshot
                    snapIds = org.getLSMDB().createSnapshot(snap.getIndices());
                } catch (InterruptedException e) {
                    throw new BabuDBException(ErrorCode.INTERRUPTED, e.getMessage());
                } finally {
                    dbs.getPersistenceManager().unlockService();
                }
                
                // then, enqueue a snapshot materialization request in the
                // checkpointer's queue
                dbs.getCheckpointer().addSnapshotMaterializationRequest(dbName, snapIds, snap);
                
                // as long as the snapshot has not been persisted yet, add a view on
                // the current snapshot in the original database to the snapshot DB map
                synchronized (snapshotDBs) {
                    
                    Snapshot s = snapMap.get(snap.getName());
                    if (s.getView() == null) {
                        s.setView(new InMemoryView(dbs, dbName, snap, snapIds));
                    }
                }
            }
        });

        dbs.getPersistenceManager().registerInMemoryProcessing(PAYLOAD_TYPE_SNAP_DELETE, 
                new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest(Object[] args) throws BabuDBException {
                
                // parse args
                String dbName = (String) args[0];
                String snapshotName = (String) args[1];
                
                byte[] data = new byte[1 + dbName.length() + 
                                       snapshotName.length()];
                byte[] dbNameBytes = dbName.getBytes();
                byte[] snapNameBytes = snapshotName.getBytes();
                
                assert (dbName.length() <= Byte.MAX_VALUE);
                data[0] = (byte) dbName.length();
                System.arraycopy(dbNameBytes, 0, data, 1, 
                        dbNameBytes.length);
                System.arraycopy(snapNameBytes, 0, data, 
                        1 + dbNameBytes.length, snapNameBytes.length);
                
                return ReusableBuffer.wrap(data);
            }
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                byte[] payload = serialized.array();
                int offs = payload[0];
                String dbName = new String(payload, 1, offs);
                String snapName = new String(payload, offs + 1, payload.length - offs - 1);
                serialized.flip();
                
                return new Object[] { dbName, snapName };
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#before(java.lang.Object[])
             */
            @Override
            public void before(Object[] args) throws BabuDBException {
                
                // parse args
                String dbName = (String) args[0];
                String snapshotName = (String) args[1];
                
                final Map<String, Snapshot> snapMap = snapshotDBs.get(dbName);
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
                dbs.getCheckpointer().removeSnapshotMaterializationRequest(dbName, snapshotName);
                
                // delete the snapshot subdirectory on disk if available
                FSUtils.delTree(new File(getSnapshotDir(dbName, snapshotName)));
            }
        });
    }
}
