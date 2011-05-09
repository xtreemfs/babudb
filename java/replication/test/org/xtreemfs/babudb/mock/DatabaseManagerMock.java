/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.foundation.logging.Logging;

/**
 * 
 * @author flangner
 * @since 02/23/2011
 */

public class DatabaseManagerMock implements DatabaseManagerInternal {

    private final Map<String, DatabaseInternal> dbsByName = new HashMap<String, DatabaseInternal>();
    private final Map<Integer, DatabaseInternal> dbsById = new HashMap<Integer, DatabaseInternal>();
    
    private final Map<String, DatabaseInternal> dbsOnDisk = new HashMap<String, DatabaseInternal>();
    
    @Override
    public void copyDatabase(String sourceDB, String destDB)
            throws BabuDBException {
        
        if (dbsByName.containsKey(sourceDB)) {
            dbsByName.put(destDB, dbsByName.get(sourceDB));
            dbsById.put(Integer.valueOf(destDB), dbsByName.get(sourceDB));
            dbsOnDisk.put(destDB, dbsByName.get(sourceDB));
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-copy failed!");
        }
    }

    @Override
    public DatabaseInternal createDatabase(String databaseName, int numIndices)
            throws BabuDBException {
        
        return createDatabase(databaseName, numIndices, null);
    }

    @Override
    public DatabaseInternal createDatabase(String databaseName, int numIndices,
            ByteRangeComparator[] comparators) throws BabuDBException {
        
        if (comparators == null) {
            comparators = new ByteRangeComparator[numIndices];
            final ByteRangeComparator defaultComparator = new DefaultByteRangeComparator();
            for (int i = 0; i < numIndices; i++) {
                comparators[i] = defaultComparator;
            }
        }
        
        if (!dbsByName.containsKey(databaseName)) {
            DatabaseInternal result = new DatabaseMock(databaseName, numIndices, comparators);
            dbsByName.put(databaseName, result);
            return result;
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-create failed!");
        }
    }

    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {
        
        if (dbsByName.containsKey(databaseName)) {
            dbsByName.remove(databaseName);
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-remove failed!");
        }
    }

    @Override
    public void dumpAllDatabases(String destPath) throws BabuDBException,
            IOException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public DatabaseInternal getDatabase(String dbName) throws BabuDBException {

        Logging.logMessage(Logging.LEVEL_ERROR, this, "Accessing dbName '%s' from mock.", dbName);
        
        if (dbsByName.containsKey(dbName)) {
            return dbsByName.get(dbName);
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-getDB(" + dbName 
                    + ") failed!");
        }
    }

    @Override
    public Map<String, Database> getDatabases() {
        Logging.logMessage(Logging.LEVEL_ERROR, this, "Retrieving all DBs from mock.");
        
        return new HashMap<String, Database>(dbsByName);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabase(int)
     */
    @Override
    public DatabaseInternal getDatabase(int dbId) throws BabuDBException {
        
        Logging.logMessage(Logging.LEVEL_ERROR, this, "Accessing dbId '%d' from mock.", dbId);
        
        if (dbsById.containsKey(dbId)) {
            return dbsById.get(dbId);
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-getDB(" + dbId + ") failed!");
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabasesInternal()
     */
    @Override
    public Map<String, DatabaseInternal> getDatabasesInternal() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDatabaseList()
     */
    @Override
    public Collection<DatabaseInternal> getDatabaseList() {
        return dbsByName.values();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getDBModificationLock()
     */
    @Override
    public Object getDBModificationLock() {
        return this;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#reset()
     */
    @Override
    public void reset() throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getNextDBId()
     */
    @Override
    public int getNextDBId() {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#setNextDBId(int)
     */
    @Override
    public void setNextDBId(int id) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getComparatorInstances()
     */
    @Override
    public Map<String, ByteRangeComparator> getComparatorInstances() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#putDatabase(org.xtreemfs.babudb.api.dev.DatabaseInternal)
     */
    @Override
    public void putDatabase(DatabaseInternal database) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#getAllDatabaseIds()
     */
    @Override
    public Set<Integer> getAllDatabaseIds() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#removeDatabaseById(int)
     */
    @Override
    public void removeDatabaseById(int id) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#executeTransaction(org.xtreemfs.babudb.api.transaction.Transaction)
     */
    @Override
    public void executeTransaction(Transaction txn) throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#addTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void addTransactionListener(TransactionListener listener) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#removeTransactionListener(org.xtreemfs.babudb.api.transaction.TransactionListener)
     */
    @Override
    public void removeTransactionListener(TransactionListener listener) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#createTransaction()
     */
    @Override
    public TransactionInternal createTransaction() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseManagerInternal#executeTransaction(org.xtreemfs.babudb.api.dev.transaction.TransactionInternal)
     */
    @Override
    public void executeTransaction(TransactionInternal txn) throws BabuDBException {
        // TODO Auto-generated method stub
        
    }
}