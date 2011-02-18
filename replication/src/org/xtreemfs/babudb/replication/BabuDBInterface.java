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
import java.util.Map;
import java.util.Vector;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * Methods that may be executed on BabuDB from the replication mechanisms.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public class BabuDBInterface {
   
    /** reference to {@link BabuDB}, will use the persistence manager proxy, when plugin is initialized */
    private final BabuDBInternal        dbs;
    
    /** the persistence manager using the local DiskLogger: NOT THE PROXY */
    private final PersistenceManager    localPersMan;
        
    /**
     * Registers the reference of local {@link BabuDB}.
     * 
     * @param babuDB - {@link BabuDB}.
     */
    public BabuDBInterface(BabuDBInternal babuDB) {
        localPersMan = babuDB.getPersistenceManager();
        dbs = babuDB;
    }
    
    /**
     * Appends the given entry to the <b>local</b> {@link DiskLogger} using the 
     * <b>local</b> {@link PersistenceManager}. For internal usage only!
     * 
     * @param entry - {@link LogEntry}.
     * @param listener - awaiting the result for this insert.
     *
     * @throws BabuDBException 
     */
    public void appendToLocalPersistenceManager(LogEntry entry, 
            DatabaseRequestListener<Object> listener) 
            throws BabuDBException {
        
        localPersMan.makePersistent(entry.getPayloadType(), 
                                    entry.getPayload()).registerListener(listener);
    }
    
    /**
     * @return the {@link LSN} of the last inserted {@link LogEntry}.
     */ 
    public LSN getState() {
        return localPersMan.getLatestOnDiskLSN();
    }
    
    /**
     * @return the {@link CheckpointerImpl} lock object.
     */
    public Object getCheckpointerLock() {
        return getChckPtr();
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
     * 
     * @return the LSN of the last LogEntry written to the DiskLog.
     */
    public LSN checkpoint() throws BabuDBException {
        return getChckPtr().checkpoint(true);
    }
    
    /**
     * @return a map of all available databases identified by their names.
     */
    public Map<String, Database> getDatabases() {
        return getDBMan().getDatabases();
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
