/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.InMemoryProcessing;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * Methods that may be executed on BabuDB from the replication mechanisms.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public class BabuDBInterface {
   
    /** reference to {@link BabuDB} */
    private final BabuDBInternal dbs;
    
    /**
     * Registers the reference of {@link BabuDB}.
     * 
     * @param babuDB - {@link BabuDB}.
     */
    public BabuDBInterface(BabuDBInternal babuDB) {
        dbs = babuDB;
    }
    
    /**
     * Appends the given entry to the local {@link DiskLogger}.
     * 
     * @param entry - {@link LogEntry}.
     * @throws InterruptedException 
     * @throws BabuDBException 
     */
    public void appendToDisklogger(final LogEntry entry) throws BabuDBException {
        dbs.getPersistenceManager().makePersistent(entry.getPayloadType(), 
                new InMemoryProcessing() {
                    
                    @Override
                    public ReusableBuffer serializeRequest() 
                            throws BabuDBException {
                        
                        return entry.getPayload();
                    }
                });
    }
    
    /**
     * Inserts the given {@link InsertRecordGroup} into the database.
     * 
     * @param irg - {@link InsertRecordGroup}.
     */
    public void insertRecordGroup(InsertRecordGroup irg) {
        getDBMan().insert(irg);
    }
    
    /**
     * @return the {@link LSN} of the last inserted {@link LogEntry}.
     */
    public LSN getState() {
        return dbs.getLogger().getLatestLSN();
    }
    
    /**
     * @param dbId
     * @return true if the database with the given ID exists, false otherwise.
     */
    public boolean dbExists(int dbId) {
        return (getDBMan().getDatabase(dbId) != null);
    }
    
    /**
     * Creates a new database using name and numOfIndices as parameters.
     * 
     * @param name
     * @param numOfIndices
     * @throws BabuDBException 
     */
    public void createDB(String name, int numOfIndices) throws BabuDBException {
        getDBMan().createDatabase(name, numOfIndices, null);
    }
    
    /**
     * Copies database from to.
     * 
     * @param from
     * @param to
     * @throws BabuDBException 
     */
    public void copyDB(String from, String to) throws BabuDBException {
        getDBMan().copyDatabase(from, to);
    }
    
    /**
     * Deletes the database given by name.
     * 
     * @param name
     * @throws BabuDBException
     */
    public void deleteDB(String name) throws BabuDBException {
        getDBMan().deleteDatabase(name);
    }
   
    
    /**
     * Creates a new snapshot for the given parameters.
     * 
     * @param dbId
     * @param snapConf
     * @throws BabuDBException
     */
    public void createSnapshot(int dbId, SnapshotConfig snapConf) 
        throws BabuDBException {
        
        getSnapMan().createPersistentSnapshot(getDBMan().getDatabase(dbId).
                getName(),snapConf, false);
    }
    
    /**
     * Deletes an existing snapshot.
     * 
     * @param dbName
     * @param snapName
     * @throws BabuDBException
     */
    public void deleteSnapshot(String dbName, String snapName) 
        throws BabuDBException {
        
        getSnapMan().deletePersistentSnapshot(dbName, snapName, false);    
    }
    
    /**
     * @return the {@link CheckpointerImpl} lock object.
     */
    public Object getCheckpointerLock() {
        return getChckPtr().getCheckpointerLock();
    }
    
    /**
     * @return the BabuDB modification lock object.
     */
    public Object getDBModificationLock() {
        return getDBMan().getDBModificationLock();
    }

    /**
     * Blockades if there actually is a checkpoint taken.
     * 
     * @throws InterruptedException 
     */
    public void waitForCheckpoint() throws InterruptedException {
        getChckPtr().waitForCheckpoint();
    }
    
    /**
     * Stops BabuDB to restart it later.
     */
    public void stopBabuDB() {
        dbs.stop();
    }
    
    /**
     * Restarts the previously stopped BabuDB.
     * 
     * @return the {@link LSN} of the last {@link LogEntry} restored.
     * @throws BabuDBException
     */
    public LSN startBabuDB() throws BabuDBException {
        return dbs.restart();
    }
    
    /**
     * Creates a new checkpoint and increments the viewId.
     * 
     * @throws BabuDBException
     */
    public void checkpoint() throws BabuDBException {
        getChckPtr().checkpoint(true);
    }
    
    /**
     * @return a set of names of all available databases.
     */
    public Set<String> getDatabases() {
        return getDBMan().getDatabases().keySet();
    }
    
    /**
     * @param dbName
     * @return an instance of the local database available for the given name.
     * @throws BabuDBException 
     */
    public Database getDatabase(String dbName) throws BabuDBException {
        return getDBMan().getDatabase(dbName);
    }
    
/*
 * private
 */
    
    /**
     * @return the {@link CheckpointerImpl} retrieved from the {@link BabuDB}.
     */
    private CheckpointerImpl getChckPtr() {
        return (CheckpointerImpl) dbs.getCheckpointer();
    }
    
    /**
     * @return the {@link DatabaseManagerImpl} retrieved from the 
     *         {@link BabuDB}.
     */
    private DatabaseManagerImpl getDBMan() {
        return (DatabaseManagerImpl) dbs.getDatabaseManager();
    }
    
    /**
     * @return the {@link SnapshotManagerImpl} retrieved from the 
     *         {@link BabuDB}.
     */
    private SnapshotManagerImpl getSnapMan() {
        return (SnapshotManagerImpl) dbs.getSnapshotManager();
    }

    /**
     * @return a collection of all {@link DBFileMetaData} objects for the latest
     *         snapshot available for every database, after cutting them into
     *         chunks of chunkSize at maximum.
     */
    public Collection<DBFileMetaData> getAllSnapshotFiles() {
        List<DBFileMetaData> result = new Vector<DBFileMetaData>(); 
        
        for (Database db : getDBMan().getDatabaseList()) {
            result.addAll(
                    ((DatabaseImpl) db).getLSMDB().getLastestSnapshotFiles());
        }
        
        return result;
    }

    /**
     * @return the {@link PersistenceManager} of the local BabuDB instance.
     */
    public PersistenceManager getPersistanceManager() {
        return dbs.getPersistenceManager();
    }
}
