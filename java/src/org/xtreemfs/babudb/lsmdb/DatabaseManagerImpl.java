/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import static org.xtreemfs.include.common.config.SlaveConfig.slaveProtection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl.AsyncResult;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.common.util.FSUtils;

public class DatabaseManagerImpl implements DatabaseManager {
    
    private BabuDB                                 dbs;
    
    /**
     * Mapping from database name to database id
     */
    private final Map<String, Database>            dbNames;
    
    /**
     * Mapping from dbId to database
     */
    private final Map<Integer, Database>           databases;
    
    /**
     * a map containing all comparators sorted by their class names
     */
    private final Map<String, ByteRangeComparator> compInstances;
    
    /**
     * ID to assign to next database create
     */
    private int                                    nextDbId;
    
    /**
     * object used for synchronizing modifications of the database lists.
     */
    private final Object                           dbModificationLock;
    
    public DatabaseManagerImpl(BabuDB master) throws BabuDBException {
        
        this.dbs = master;
        
        this.dbNames = new HashMap<String, Database>();
        this.databases = new HashMap<Integer, Database>();
        
        this.compInstances = new HashMap<String, ByteRangeComparator>();
        this.compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());
        
        this.nextDbId = 1;
        this.dbModificationLock = new Object();
        
        loadDBs();
    }
    
    public void reset() throws BabuDBException {
        
        dbNames.clear();
        databases.clear();
        
        nextDbId = 1;
        
        compInstances.clear();
        compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());
        
        loadDBs();
        
        synchronized (dbModificationLock) {
            dbModificationLock.notifyAll();
        }
    }
    
    public Collection<Database> getDatabases() {
        synchronized (dbModificationLock) {
            return new ArrayList<Database>(databases.values());
        }
    }
    
    public Map<String, Database> getDatabaseNameMap() {
        synchronized (dbModificationLock) {
            return new HashMap<String, Database>(dbNames);
        }
    }
    
    public Map<Integer, Database> getDatabaseMap() {
        synchronized (dbModificationLock) {
            return new HashMap<Integer, Database>(databases);
        }
    }
    
    @Override
    public Database getDatabase(String dbName) throws BabuDBException {
        
        Database db = dbNames.get(dbName);
        
        if (db == null)
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        return db;
    }
    
    public Database getDatabase(int dbId) {
        return databases.get(dbId);
    }
    
    @Override
    public Database createDatabase(String databaseName, int numIndices) throws BabuDBException {
        return createDatabase(databaseName, numIndices, null);
    }
    
    @Override
    public Database createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators)
        throws BabuDBException {
        
        if (dbs.replication_isSlave()) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, slaveProtection);
        }
        
        return proceedCreate(databaseName, numIndices, comparators);
    }
    
    /**
     * Proceeds a Create, without isSlaveCheck. Replication Approach!
     * 
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
        synchronized (dbModificationLock) {
            if (dbNames.containsKey(databaseName)) {
                throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + databaseName
                    + "' already exists");
            }
            final int dbId = nextDbId++;
            db = new DatabaseImpl(dbs, new LSMDatabase(databaseName, dbId, dbs.getConfig().getBaseDir()
                + databaseName + File.separatorChar, numIndices, false, comparators));
            databases.put(dbId, db);
            dbNames.put(databaseName, db);
            saveDBconfig();
        }
        
        // if this is a master it sends the create-details to all slaves.
        // otherwise nothing happens
        if (dbs.getReplicationManager() != null && dbs.getReplicationManager().isMaster()) {
            dbs.getReplicationManager().create(noOpInsert(), databaseName, numIndices);
        }
        
        return db;
    }
    
    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {
        synchronized (((CheckpointerImpl) dbs.getCheckpointer()).getDeleteLock()) {
            deleteDatabase(databaseName, true);
        }
    }
    
    /**
     * Deletes a database.
     * 
     * @param databaseName
     *            name of database to delete
     * @param deleteFiles
     *            if true, all checkpoints are deleted as well
     * @throws BabuDBException
     */
    public void deleteDatabase(String databaseName, boolean deleteFiles) throws BabuDBException {
        if (dbs.replication_isSlave()) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, slaveProtection);
        }
        
        proceedDelete(databaseName, deleteFiles);
    }
    
    /**
     * Proceeds a Delete, without isSlaveCheck. Replication Approach!
     * 
     * @param databaseName
     * @param deleteFiles
     * @throws BabuDBException
     */
    public void proceedDelete(String databaseName, boolean deleteFiles) throws BabuDBException {
        synchronized (dbModificationLock) {
            if (!dbNames.containsKey(databaseName)) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + databaseName
                    + "' does not exists");
            }
            final LSMDatabase db = ((DatabaseImpl) dbNames.get(databaseName)).getLSMDB();
            dbNames.remove(databaseName);
            databases.remove(db.getDatabaseId());
            
            ((SnapshotManagerImpl) dbs.getSnapshotManager()).deleteAllSnapshots(databaseName, deleteFiles);
            
            saveDBconfig();
            if (deleteFiles) {
                File dbDir = new File(dbs.getConfig().getBaseDir(), databaseName);
                if (dbDir.exists())
                    FSUtils.delTree(dbDir);
            }
        }
        
        // if this is a master it sends the delete-details to all slaves.
        // otherwise nothing happens
        if (dbs.getReplicationManager() != null && dbs.getReplicationManager().isMaster()) {
            dbs.getReplicationManager().delete(noOpInsert(), databaseName, deleteFiles);
        }
    }
    
    @Override
    public void copyDatabase(String sourceDB, String destDB) throws BabuDBException, IOException,
        InterruptedException {
        if (dbs.replication_isSlave()) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, slaveProtection);
        }
        
        proceedCopy(sourceDB, destDB);
    }
    
    /**
     * Proceeds a Copy, without isSlaveCheck. Replication Approach!
     * 
     * @param sourceDB
     * @param destDB
     * @throws BabuDBException
     * @throws IOException
     */
    public void proceedCopy(String sourceDB, String destDB) throws BabuDBException, InterruptedException {
        
        final DatabaseImpl sDB = (DatabaseImpl) dbNames.get(sourceDB);
        if (sDB == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + sourceDB + "' does not exist");
        }
        
        final int dbId;
        synchronized (dbModificationLock) {
            if (dbNames.containsKey(destDB)) {
                throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + destDB + "' already exists");
            }
            dbId = nextDbId++;
            // just "reserve" the name
            dbNames.put(destDB, null);
            saveDBconfig();
            
        }
        // materializing the snapshot takes some time, we should not hold the
        // lock meanwhile!
        sDB.proceedSnapshot(destDB);
        
        // create new DB and load from snapshot
        Database newDB = new DatabaseImpl(dbs, new LSMDatabase(destDB, dbId, dbs.getConfig().getBaseDir()
            + destDB + File.separatorChar, sDB.getLSMDB().getIndexCount(), true, sDB.getComparators()));
        
        // insert real database
        synchronized (dbModificationLock) {
            databases.put(dbId, newDB);
            dbNames.put(destDB, newDB);
            saveDBconfig();
        }
        
        // if this is a master it sends the copy-details to all slaves.
        // otherwise nothing happens
        if (dbs.getReplicationManager() != null && dbs.getReplicationManager().isMaster()) {
            dbs.getReplicationManager().copy(noOpInsert(), sourceDB, destDB);
        }
    }
    
    public void shutdown() throws BabuDBException {
        for (Database db : databases.values())
            db.shutdown();
    }
    
    /**
     * Loads the configuration and each database from disk
     * 
     * @throws BabuDBException
     */
    private void loadDBs() throws BabuDBException {
        try {
            File f = new File(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile());
            if (f.exists()) {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
                final int dbFormatVer = ois.readInt();
                if (dbFormatVer != BabuDB.BABUDB_DB_FORMAT_VERSION) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "on-disk format (version " + dbFormatVer
                        + ") is incompatible with this BabuDB release " + "(uses on-disk format version "
                        + BabuDB.BABUDB_DB_FORMAT_VERSION + ")");
                }
                final int numDB = ois.readInt();
                nextDbId = ois.readInt();
                for (int i = 0; i < numDB; i++) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading DB...");
                    final String dbName = (String) ois.readObject();
                    final int dbId = ois.readInt();
                    final int numIndex = ois.readInt();
                    ByteRangeComparator[] comps = new ByteRangeComparator[numIndex];
                    for (int idx = 0; idx < numIndex; idx++) {
                        final String className = (String) ois.readObject();
                        ByteRangeComparator comp = compInstances.get(className);
                        if (comp == null) {
                            Class<?> clazz = Class.forName(className);
                            comp = (ByteRangeComparator) clazz.newInstance();
                            compInstances.put(className, comp);
                        }
                        
                        assert (comp != null);
                        comps[idx] = comp;
                    }
                    
                    Database db = new DatabaseImpl(dbs, new LSMDatabase(dbName, dbId, dbs.getConfig()
                            .getBaseDir()
                        + dbName + File.separatorChar, numIndex, true, comps));
                    databases.put(dbId, db);
                    dbNames.put(dbName, db);
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "loaded DB " + dbName + " successfully.");
                }
                
                ois.close();
            }
            
        } catch (InstantiationException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IllegalAccessException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, check path and access rights", ex);
        } catch (ClassNotFoundException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, config file might be corrupted", ex);
        } catch (ClassCastException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database config, config file might be corrupted", ex);
        }
        
    }
    
    /**
     * <p>
     * Makes an empty logEntry-insert at the {@link DiskLogger} that will not be
     * replayed, to get an unique {@link LSN} for a serviceCall.
     * </p>
     * 
     * @return the unique {@link LSN}.
     * @throws BabuDBException
     *             if dummy {@link LogEntry} could not be appended to the
     *             DiskLogger.
     */
    private LSN noOpInsert() throws BabuDBException {
        final AsyncResult result = new AsyncResult();
        
        // make the dummy entry
        LogEntry dummy = new LogEntry(ReusableBuffer.wrap(new byte[0]), new SyncListener() {
            
            @Override
            public void synced(LogEntry entry) {
                synchronized (result) {
                    result.done = true;
                    result.notify();
                }
            }
            
            @Override
            public void failed(LogEntry entry, Exception ex) {
                synchronized (result) {
                    result.done = true;
                    result.error = new BabuDBException(ErrorCode.INTERNAL_ERROR, ex.getMessage());
                    result.notify();
                }
            }
        }, LogEntry.PAYLOAD_TYPE_INSERT);
        
        // append it to the DiskLogger
        try {
            dbs.getLogger().append(dummy);
            
            synchronized (result) {
                if (!result.done)
                    result.wait();
                
                if (result.error != null)
                    throw result.error;
            }
        } catch (InterruptedException ie) {
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR, ie.getMessage());
        }
        
        // return its LSN
        return dummy.getLSN();
    }
    
    /**
     * saves the current database config to disk
     * 
     * @throws BabuDBException
     */
    private void saveDBconfig() throws BabuDBException {
        /*
         * File f = new File(baseDir+DBCFG_FILE); f.renameTo(new
         * File(baseDir+DBCFG_FILE+".old"));
         */
        synchronized (dbModificationLock) {
            try {
                FileOutputStream fos = new FileOutputStream(dbs.getConfig().getBaseDir()
                    + dbs.getConfig().getDbCfgFile() + ".in_progress");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeInt(BabuDB.BABUDB_DB_FORMAT_VERSION);
                oos.writeInt(databases.size());
                oos.writeInt(nextDbId);
                for (int dbId : databases.keySet()) {
                    LSMDatabase db = ((DatabaseImpl) databases.get(dbId)).getLSMDB();
                    oos.writeObject(db.getDatabaseName());
                    oos.writeInt(dbId);
                    oos.writeInt(db.getIndexCount());
                    String[] compClasses = db.getComparatorClassNames();
                    for (int i = 0; i < db.getIndexCount(); i++) {
                        oos.writeObject(compClasses[i]);
                    }
                }
                
                oos.flush();
                fos.flush();
                fos.getFD().sync();
                oos.close();
                File f = new File(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile()
                    + ".in_progress");
                f.renameTo(new File(dbs.getConfig().getBaseDir() + dbs.getConfig().getDbCfgFile()));
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "unable to save database configuration", ex);
            }
            
        }
    }
    
}
