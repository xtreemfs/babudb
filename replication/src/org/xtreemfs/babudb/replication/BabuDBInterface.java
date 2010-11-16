/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication;

import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;

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
        this.dbs = babuDB;
    }
    
    /**
     * Appends the given entry to the local {@link DiskLogger}.
     * 
     * @param entry - {@link LogEntry}.
     * @throws InterruptedException 
     */
    public void appendToDisklogger(LogEntry entry) throws InterruptedException {
        this.dbs.getLogger().append(entry);
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
        return this.dbs.getLogger().getLatestLSN();
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
        getDBMan().proceedCreate(name, numOfIndices, null);
    }
    
    /**
     * Copies database from to.
     * 
     * @param from
     * @param to
     * @throws BabuDBException 
     */
    public void copyDB(String from, String to) throws BabuDBException {
        getDBMan().proceedCopy(from, to);
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
        this.dbs.stop();
    }
    
    /**
     * Restarts the previously stopped BabuDB.
     * 
     * @return the {@link LSN} of the last {@link LogEntry} restored.
     * @throws BabuDBException
     */
    public LSN startBabuDB() throws BabuDBException {
        return this.dbs.restart();
    }
    
    /**
     * Creates a new checkpoint and increments the viewId.
     * 
     * @throws BabuDBException
     */
    public void checkpoint() throws BabuDBException {
        getChckPtr().checkpoint(true);
    }
    
/*
 * private
 */
    
    /**
     * @return the {@link CheckpointerImpl} retrieved from the {@link BabuDB}.
     */
    private CheckpointerImpl getChckPtr() {
        return (CheckpointerImpl) this.dbs.getCheckpointer();
    }
    
    /**
     * @return the {@link DatabaseManagerImpl} retrieved from the 
     *         {@link BabuDB}.
     */
    private DatabaseManagerImpl getDBMan() {
        return (DatabaseManagerImpl) this.dbs.getDatabaseManager();
    }
    
    /**
     * @return the {@link SnapshotManagerImpl} retrieved from the 
     *         {@link BabuDB}.
     */
    private SnapshotManagerImpl getSnapMan() {
        return (SnapshotManagerImpl) this.dbs.getSnapshotManager();
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
                    ((DatabaseImpl) db).getLSMDB().getLastestSnapshotFiles(
                            ((ReplicationConfig) this.dbs.getConfig())
                                .getChunkSize())
                         );
        }
        
        return result;
    }
}
