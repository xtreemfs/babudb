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

import org.xtreemfs.babudb.BabuDBRequestResultImpl;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.Checkpointer;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.api.database.DatabaseRequestListener;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.CheckpointerInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.proxy.DatabaseManagerProxy;
import org.xtreemfs.babudb.replication.proxy.DatabaseProxy;
import org.xtreemfs.babudb.replication.proxy.TransactionManagerProxy;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Methods that may be executed on BabuDB from the replication mechanisms.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public class BabuDBInterface {
   
    /** reference to {@link BabuDB}, will use the persistence manager proxy, when plugin is initialized */
    private final BabuDBInternal                dbs;
    
    /** the persistence manager using the local DiskLogger: NOT THE PROXY */
    private final TransactionManagerInternal    localTxnMan;
    
    private DatabaseManagerProxy                dbMan;
        
    /**
     * Registers the reference of local {@link BabuDB}.
     * 
     * @param babuDB - {@link BabuDB}.
     */
    public BabuDBInterface(BabuDBInternal babuDB) {
        localTxnMan = babuDB.getTransactionManager();
        dbs = babuDB;
    }
    
    public void init(DatabaseManagerProxy dbMan) {
        this.dbMan = dbMan;
    }
    
    /**
     * Appends the given entry to the <b>local</b> {@link DiskLogger} using the 
     * <b>local</b> {@link PersistenceManager}. For internal usage only!
     * 
     * @param entry - {@link LogEntry}.
     * @param listener - awaiting the result for this insert.
     *
     * @throws BabuDBException if appending failed.
     */
    public void appendToLocalPersistenceManager(LogEntry entry, DatabaseRequestListener<Object> listener) 
            throws BabuDBException {
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Appending entry to logger: %s", 
                new String(entry.getPayload().array()));
        
        BabuDBRequestResultImpl<Object> future = new BabuDBRequestResultImpl<Object>(dbs.getResponseManager());
        localTxnMan.makePersistent(entry.getPayload(), future);
        future.registerListener(listener);
    }
    
    /**
     * @return the {@link LSN} of the last inserted {@link LogEntry}.
     */ 
    public LSN getState() {
        return localTxnMan.getLatestOnDiskLSN();
    }
    
    /**
     * @return the {@link Checkpointer} lock object.
     */
    public Object getCheckpointerLock() {
        return getChckPtr();
    }
    
    /**
     * @return the BabuDB modification lock object.
     */
    public Object getDBModificationLock() {
        return dbs.getDatabaseManager().getDBModificationLock();
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
     * @throws BabuDBException 
     */
    public Map<String, DatabaseProxy> getDatabases() {
        return dbMan.getDatabasesInternalNonblocking();
    }
    
    /**
     * @param dbName
     * @return an instance of the local database available for the given name.
     * @throws BabuDBException 
     */
    public DatabaseProxy getDatabase(String dbName) throws BabuDBException {
        return dbMan.getDatabaseNonblocking(dbName);
    }
    
    /**
     * @param dbId
     * @return an instance of the local database available for the given identifier.
     * @throws BabuDBException 
     */
    public DatabaseProxy getDatabase(int dbId) throws BabuDBException {
        return dbMan.getDatabaseNonblocking(dbId);
    }
    
/*
 * private
 */
    
    /**
     * @return the {@link CheckpointerInternal} retrieved from the {@link BabuDB}.
     */
    private CheckpointerInternal getChckPtr() {
        return dbs.getCheckpointer();
    }

    /**
     * @return a collection of all {@link DBFileMetaData} objects for the latest
     *         snapshot available for every database, after cutting them into
     *         chunks of chunkSize at maximum.
     */
    public Collection<DBFileMetaData> getAllSnapshotFiles() {
        List<DBFileMetaData> result = new Vector<DBFileMetaData>(); 
        
        for (DatabaseProxy db : dbMan.getDatabaseListNonblocking()) {
            result.addAll(db.getLSMDB().getLastestSnapshotFiles());
        }
        
        return result;
    }

    /**
     * @return the {@link TransactionManagerProxy} of the local BabuDB instance.
     */
    public TransactionManagerProxy getTransactionManager() {
        return (TransactionManagerProxy) dbs.getTransactionManager();
    }
    
    /**
     * @return a {@link BabuDBRequestResultImpl} object.
     */
    public BabuDBRequestResultImpl<Object> createRequestFuture() {
        return new BabuDBRequestResultImpl<Object>(dbs.getResponseManager());
    }
}
