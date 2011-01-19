/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import static org.xtreemfs.babudb.log.LogEntry.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
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
    }
    
    public void reset() throws BabuDBException {
        nextDbId = 1;
        
        compInstances.clear();
        compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());
        
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
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(java.lang.String, int, org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators)
        throws BabuDBException {
        
        return proceedCreate(databaseName, numIndices, comparators);
    }
    
    /**
     * @param databaseName
     * @param numIndices
     * @param comparators
     *            - if null - default comparators will be used.
     * @return the newly created database
     * @throws BabuDBException
     */
    public Database proceedCreate(String databaseName, int numIndices, ByteRangeComparator[] comparators)
        throws BabuDBException {
        
        if (comparators == null) {
            ByteRangeComparator[] comps = new ByteRangeComparator[numIndices];
            final ByteRangeComparator defaultComparator = compInstances.get(DefaultByteRangeComparator.class
                    .getName());
            for (int i = 0; i < numIndices; i++) {
                comps[i] = defaultComparator;
            }
            
            comparators = comps;
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
                        + File.separatorChar, numIndices, false, comparators, dbs.getConfig().getCompression(),
                        dbs.getConfig().getMaxNumRecordsPerBlock(), dbs.getConfig().getMaxBlockFileSize(), dbs
                                .getConfig().getDisableMMap(), dbs.getConfig().getMMapLimit()));
                dbsById.put(dbId, db);
                dbsByName.put(databaseName, db);
                dbs.getDBConfigFile().save();
            }
        }
        
        // append the data to the diskLogger
        ReusableBuffer buf = ReusableBuffer.wrap(new byte[databaseName.getBytes().length
            + (Integer.SIZE * 3 / 8)]);
        buf.putInt(db.getLSMDB().getDatabaseId());
        buf.putString(databaseName);
        buf.putInt(numIndices);
        buf.flip();
        
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_CREATE, buf)
                                   .get();        
        return db;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#deleteDatabase(
     *                  java.lang.String)
     */
    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {        
        proceedDelete(databaseName);
    }
    
    /**
     * @param databaseName
     * @throws BabuDBException
     */
    public void proceedDelete(String databaseName) throws BabuDBException {
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
        
        // append the data to the diskLogger
        ReusableBuffer buf = ReusableBuffer
                .wrap(new byte[(Integer.SIZE / 2) + databaseName.getBytes().length]);
        buf.putInt(dbId);
        buf.putString(databaseName);
        buf.flip();
        
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_DELETE, buf)
                                   .get();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#copyDatabase(java.lang.String, java.lang.String)
     */
    @Override
    public void copyDatabase(String sourceDB, String destDB) 
            throws BabuDBException {        
        proceedCopy(sourceDB, destDB);
    }
    
    /**
     * @param sourceDB
     * @param destDB
     * @throws BabuDBException
     */
    public void proceedCopy(String sourceDB, String destDB) throws BabuDBException {
        
        final DatabaseImpl sDB = (DatabaseImpl) dbsByName.get(sourceDB);
        if (sDB == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + sourceDB + "' does not exist");
        }
        
        final int dbId;
        synchronized (getDBModificationLock()) {
            synchronized (dbs.getCheckpointer()) {
                if (dbsByName.containsKey(destDB)) {
                    throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + destDB + "' already exists");
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
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "Snapshot creation was interrupted.", i);
        }
        
        // create new DB and load from snapshot
        Database newDB = new DatabaseImpl(dbs, new LSMDatabase(destDB, dbId, dbs.getConfig().getBaseDir()
            + destDB + File.separatorChar, sDB.getLSMDB().getIndexCount(), true, sDB.getComparators(), dbs
                .getConfig().getCompression(), dbs.getConfig().getMaxNumRecordsPerBlock(), dbs.getConfig()
                .getMaxBlockFileSize(), dbs.getConfig().getDisableMMap(), this.dbs.getConfig().getMMapLimit()));
        
        // insert real database
        synchronized (dbModificationLock) {
            dbsById.put(dbId, newDB);
            dbsByName.put(destDB, newDB);
            dbs.getDBConfigFile().save();
        }
        
        // append the data to the diskLogger
        ReusableBuffer buf = ReusableBuffer.wrap(new byte[(Integer.SIZE / 2) + sourceDB.getBytes().length
            + destDB.getBytes().length]);
        buf.putInt(sDB.getLSMDB().getDatabaseId());
        buf.putInt(dbId);
        buf.putString(sourceDB);
        buf.putString(destDB);
        buf.flip();
        
        dbs.getPersistenceManager().makePersistent(PAYLOAD_TYPE_COPY, buf).get();
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
}
