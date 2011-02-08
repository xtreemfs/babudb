/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import static org.xtreemfs.babudb.log.LogEntry.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.InMemoryProcessing;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

public class DatabaseManagerImpl implements DatabaseManager {
    
    private BabuDBInternal                 dbs;
    
    /**
     * Mapping from database name to database id
     */
    final Map<String, Database>            dbsByName;
    
    /**
     * Mapping from dbId to database
     */
    final Map<Integer, Database>           dbsById;
    
    /**
     * a map containing all comparators sorted by their class names
     */
    final Map<String, ByteRangeComparator> compInstances;
    
    /**
     * ID to assign to next database create
     */
    int                                    nextDbId;
    
    /**
     * object used for synchronizing modifications of the database lists.
     */
    private final Object                   dbModificationLock;
    
    public DatabaseManagerImpl(BabuDBImpl dbs) throws BabuDBException {
        
        this.dbs = dbs;
        
        this.dbsByName = new HashMap<String, Database>();
        this.dbsById = new HashMap<Integer, Database>();
        
        this.compInstances = new HashMap<String, ByteRangeComparator>();
        this.compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());
        
        this.nextDbId = 1;
        this.dbModificationLock = new Object();
        
        initializePersistenceManager();
    }
    
    public void reset() throws BabuDBException {
        nextDbId = 1;
        
        compInstances.clear();
        compInstances.put(DefaultByteRangeComparator.class.getName(), 
                          new DefaultByteRangeComparator());
        
        dbs.getDBConfigFile().reset();
    }
    
    @Override
    public Map<String, Database> getDatabases() {
        synchronized (dbModificationLock) {
            return new HashMap<String, Database>(dbsByName);
        }
    }
    
    public Map<Integer, Database> getDatabasesById() {
        synchronized (dbModificationLock) {
            return new HashMap<Integer, Database>(dbsById);
        }
    }
    
    public Collection<Database> getDatabaseList() {
        synchronized (dbModificationLock) {
            return new ArrayList<Database>(dbsById.values());
        }
    }
    
    @Override
    public Database getDatabase(String dbName) throws BabuDBException {
        
        Database db = dbsByName.get(dbName);
        
        if (db == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        return db;
    }
    
    public Database getDatabase(int dbId) {
        return dbsById.get(dbId);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(java.lang.String, int)
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices) throws BabuDBException {
        return createDatabase(databaseName, numIndices, null);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(java.lang.String, int, 
     *          org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices, 
            ByteRangeComparator[] comparators) throws BabuDBException {
        
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_CREATE, 
                new Object[] { databaseName, numIndices, comparators }).get();  
     
        return dbsByName.get(databaseName);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#deleteDatabase(java.lang.String)
     */
    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {        
        
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_DELETE, 
                new Object[]{ databaseName }).get();
    }
        
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#copyDatabase(java.lang.String, java.lang.String)
     */
    @Override
    public void copyDatabase(String sourceDB, String destDB) 
            throws BabuDBException {  
                
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_COPY, 
                                                   new Object[] { sourceDB, destDB }).get();
    }
    
    public void shutdown() throws BabuDBException {
        for (Database db : dbsById.values())
            db.shutdown();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "DB manager shut down successfully");
    }
    
    /**
     * Insert a full record group. Only to be used by log replay.
     * 
     * @param ins
     */
    public void insert(InsertRecordGroup ins) {
        final DatabaseImpl database = ((DatabaseImpl) getDatabase(ins.getDatabaseId()));
        // ignore deleted databases when recovering!
        if (database == null) {
            return;
        }
        
        LSMDatabase db = database.getLSMDB();
        
        for (InsertRecord ir : ins.getInserts()) {
            LSMTree tree = db.getIndex(ir.getIndexId());
//            Logging.logMessage(Logging.LEVEL_DEBUG, this, "insert %s=%s into %s  %d",
//                new String(ir.getKey()), (ir.getValue() == null ? "null" : new String(ir.getValue())), db
//                        .getDatabaseName(), ir.getIndexId());
            tree.insert(ir.getKey(), ir.getValue());
        }
        
    }
    
    public Object getDBModificationLock() {
        return dbModificationLock;
    }
    
    public void dumpAllDatabases(String destPath) throws BabuDBException, InterruptedException, IOException {
        // create a snapshot of each database materialized with the destPath as
        // baseDir
        destPath = destPath.endsWith(File.separator) ? destPath : destPath + File.separator;
        
        File dir = new File(destPath);
        if (!dir.exists() && !dir.mkdirs())
            throw new IOException("Directory doesnt exist and cannot be created:'" + destPath + "'");
        
        BabuDBConfig cfg = dbs.getConfig();
        dbs.getDBConfigFile().save(destPath + cfg.getDbCfgFile());
        
        for (Database db : dbsByName.values()) {
            ((DatabaseImpl) db).dumpSnapshot(destPath);
        }
    }
    
    /**
     * Feed the persistenceManager with the knowledge to handle database-modifying related requests.
     */
    private void initializePersistenceManager() {
        
        dbs.getPersistenceManager().registerInMemoryProcessing(PAYLOAD_TYPE_CREATE, 
                new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest(Object[] args) throws BabuDBException {
                
                // parse args
                String databaseName = (String) args[0];
                int numIndices = (Integer) args[1];
                //ByteRangeComparator[] com = (ByteRangeComparator[]) args[2];
                
                ReusableBuffer buf = ReusableBuffer.wrap(
                        new byte[databaseName.getBytes().length
                                 + (Integer.SIZE * 3 / 8)]);
                buf.putInt(-1); //TODO remove field (deprecated)
                buf.putString(databaseName);
                buf.putInt(numIndices);
                // TODO add ByteRangeComparators to logEntry - if (com != null) buf.putObject(com);
                buf.flip();
                
                return buf;
            }
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {

                serialized.getInt(); // do not use, deprecated
                
                String dbName = serialized.getString();
                int indices = serialized.getInt();

                return new Object[] { dbName, indices, null };
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#before(java.lang.Object[])
             */
            @Override
            public void before(Object[] args) throws BabuDBException {
             
                // parse args
                String databaseName = (String) args[0];
                int numIndices = (Integer) args[1];
                ByteRangeComparator[] com = (ByteRangeComparator[]) args[2];
                
                if (com == null) {
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndices];
                    final ByteRangeComparator defaultComparator = compInstances.get(
                            DefaultByteRangeComparator.class.getName());
                    for (int i = 0; i < numIndices; i++) {
                        comps[i] = defaultComparator;
                    }
                    
                    com = comps;
                }
                
                DatabaseImpl db = null;
                synchronized (getDBModificationLock()) {
                    synchronized (dbs.getCheckpointer()) {
                        if (dbsByName.containsKey(databaseName)) {
                            throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" 
                                    + databaseName + "' already exists");
                        }
                        final int dbId = nextDbId++;
                        db = new DatabaseImpl(dbs,
                            new LSMDatabase(databaseName, dbId, dbs.getConfig().getBaseDir() + databaseName
                                + File.separatorChar, numIndices, false, com, dbs.getConfig().getCompression(),
                                dbs.getConfig().getMaxNumRecordsPerBlock(), dbs.getConfig().getMaxBlockFileSize(), dbs
                                        .getConfig().getDisableMMap(), dbs.getConfig().getMMapLimit()));
                        dbsById.put(dbId, db);
                        dbsByName.put(databaseName, db);
                        dbs.getDBConfigFile().save();
                    }
                }
            }
        });
        
        dbs.getPersistenceManager().registerInMemoryProcessing(PAYLOAD_TYPE_DELETE, 
                new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest(Object[] args) throws BabuDBException {
                
                // parse args
                String databaseName = (String) args[0];
                
                ReusableBuffer buf = ReusableBuffer.wrap(
                        new byte[(Integer.SIZE / 2) 
                                 + databaseName.getBytes().length]);
                buf.putInt(-1); //TODO remove field (deprecated)
                buf.putString(databaseName);
                buf.flip();
                
                return buf;
            }
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
              
                serialized.getInt(); // do not use, deprecated
                
                String dbName = serialized.getString();
                
                return new Object[] { dbName };
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#before(java.lang.Object[])
             */
            @Override
            public void before(Object[] args) throws BabuDBException {
                
                // parse args
                String databaseName = (String) args[0];
                
                int dbId = -1;
                synchronized (getDBModificationLock()) {
                    synchronized (dbs.getCheckpointer()) {
                        if (!dbsByName.containsKey(databaseName)) {
                            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + databaseName
                                + "' does not exists");
                        }
                        final LSMDatabase db = ((DatabaseImpl) dbsByName.get(databaseName)).getLSMDB();
                        dbId = db.getDatabaseId();
                        dbsByName.remove(databaseName);
                        dbsById.remove(dbId);
                        
                        ((SnapshotManagerImpl) dbs.getSnapshotManager()).deleteAllSnapshots(databaseName);
                        
                        dbs.getDBConfigFile().save();
                        File dbDir = new File(dbs.getConfig().getBaseDir(), databaseName);
                        if (dbDir.exists())
                            FSUtils.delTree(dbDir);
                    }
                }
            }
        });
        
        dbs.getPersistenceManager().registerInMemoryProcessing(PAYLOAD_TYPE_COPY, 
                new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest(Object[] args) throws BabuDBException {
                
                // parse args
                String sourceDB = (String) args[0];
                String destDB = (String) args[1];
                
                ReusableBuffer buf = ReusableBuffer.wrap(
                        new byte[(Integer.SIZE / 2) 
                                 + sourceDB.getBytes().length
                                 + destDB.getBytes().length]);
                buf.putInt(-1); //TODO remove field (deprecated)
                buf.putInt(-1); //TODO remove field (deprecated)
                buf.putString(sourceDB);
                buf.putString(destDB);
                buf.flip();
                
                return buf;
            }
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                serialized.getInt(); // do not use, deprecated
                serialized.getInt(); // do not use, deprecated
                
                String sourceDB = serialized.getString();
                String destDB = serialized.getString();
                return new Object[] { sourceDB, destDB };
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#before(java.lang.Object[])
             */
            @Override
            public void before(Object[] args) throws BabuDBException {
                
                // parse args
                String sourceDB = (String) args[0];
                String destDB = (String) args[1];
                
                DatabaseImpl sDB = (DatabaseImpl) dbsByName.get(sourceDB);
                if (sDB == null) {
                    throw new BabuDBException(
                            ErrorCode.NO_SUCH_DB, "database '" + 
                            sourceDB + "' does not exist");
                }
                
                int dbId;
                synchronized (getDBModificationLock()) {
                    synchronized (dbs.getCheckpointer()) {
                        if (dbsByName.containsKey(destDB)) {
                            throw new BabuDBException(
                                    ErrorCode.DB_EXISTS, "database '" + 
                                    destDB + "' already exists");
                        }
                        dbId = nextDbId++;
                        // just "reserve" the name
                        dbsByName.put(destDB, null);
                        dbs.getDBConfigFile().save();
                    }
                }
                // materializing the snapshot takes some time, we should not hold the
                // lock meanwhile!
                try {
                    sDB.proceedSnapshot(destDB);
                } catch (InterruptedException i) {
                    throw new BabuDBException(ErrorCode.INTERNAL_ERROR, 
                            "Snapshot creation was interrupted.", i);
                }
                
                // create new DB and load from snapshot
                Database newDB = new DatabaseImpl(dbs, new LSMDatabase(destDB, dbId, dbs.getConfig().getBaseDir()
                    + destDB + File.separatorChar, sDB.getLSMDB().getIndexCount(), true, sDB.getComparators(), dbs
                        .getConfig().getCompression(), dbs.getConfig().getMaxNumRecordsPerBlock(), dbs.getConfig()
                        .getMaxBlockFileSize(), dbs.getConfig().getDisableMMap(), dbs.getConfig().getMMapLimit()));
                
                // insert real database
                synchronized (dbModificationLock) {
                    dbsById.put(dbId, newDB);
                    dbsByName.put(destDB, newDB);
                    dbs.getDBConfigFile().save();
                }
            }
        });
        
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
                    oout.writeObject(snap);
                    buf = ReusableBuffer.wrap(bout.toByteArray());
                    oout.close();
                } catch (IOException exc) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "could not serialize snapshot configuration: "
                        + snap.getClass(), exc);
                }
                
                return buf;
            }

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
                            "Could not deserialize operation of type " + PAYLOAD_TYPE_SNAP + 
                                ", because: "+e.getMessage(), e);
                } finally {
                    try {
                        if (oin != null) oin.close();
                    } catch (IOException ioe) {
                        /* who cares? */
                    }
                }
            }
        });
        
        dbs.getPersistenceManager().registerInMemoryProcessing(PAYLOAD_TYPE_INSERT, 
                new InMemoryProcessing() {
            
            @Override
            public ReusableBuffer serializeRequest(Object[] args) throws BabuDBException {
                
                // parse args
                BabuDBInsertGroup irg = (BabuDBInsertGroup) args[0];
                
                int size = irg.getRecord().getSize();
                ReusableBuffer buf = BufferPool.allocate(size);
                irg.getRecord().serialize(buf);
                buf.flip();
                
                return buf;
            }
            
            @Override
            public Object[] deserializeRequest(ReusableBuffer serialized) throws BabuDBException {
                
                InsertRecordGroup irg = InsertRecordGroup.deserialize(serialized);
                
                return new Object[] { irg, null, null };
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#before(java.lang.Object[])
             */
            @Override
            public void before(Object[] args) throws BabuDBException {
                
                // parse args
                BabuDBInsertGroup irg = (BabuDBInsertGroup) args[0];
                LSMDatabase lsmDB = (LSMDatabase) args[1];
                if (lsmDB == null) {
                    lsmDB = ((DatabaseImpl) dbsById.get(irg.getRecord().getDatabaseId())).getLSMDB();
                }
                
                int numIndices = lsmDB.getIndexCount();
                
                for (InsertRecord ir : irg.getRecord().getInserts()) {
                    if ((ir.getIndexId() >= numIndices) || 
                        (ir.getIndexId() < 0)) {
                        
                        throw new BabuDBException(
                                ErrorCode.NO_SUCH_INDEX, "index " 
                                + ir.getIndexId() + 
                                " does not exist");
                    }
                }
            }
            
            /* (non-Javadoc)
             * @see org.xtreemfs.babudb.api.InMemoryProcessing#after(java.lang.Object[])
             */
            @SuppressWarnings("unchecked")
            @Override
            public void after(Object[] args) {
                
                // parse args
                BabuDBInsertGroup irg = (BabuDBInsertGroup) args[0];
                LSMDatabase lsmDB = (LSMDatabase) args[1];
                if (lsmDB == null) {
                    lsmDB = ((DatabaseImpl) dbsById.get(irg.getRecord().getDatabaseId())).getLSMDB();
                }
                BabuDBRequestResultImpl<Object> listener = 
                    (BabuDBRequestResultImpl<Object>) args[2];
                
                // in case of synchronous inserts, wait until the disk logger 
                // returns
                if (dbs.getConfig().getSyncMode() != SyncMode.ASYNC) {
                
                    // insert into the in-memory-tree
                    for (InsertRecord ir : irg.getRecord().getInserts()) {
                        LSMTree index = lsmDB.getIndex(
                                ir.getIndexId());
                        
                        if (ir.getValue() != null) {
                            index.insert(ir.getKey(), 
                                         ir.getValue());
                        } else {
                            index.delete(ir.getKey());
                        }
                    }
                    if (listener != null) listener.finished();
                }
            }
        });
    }
}
