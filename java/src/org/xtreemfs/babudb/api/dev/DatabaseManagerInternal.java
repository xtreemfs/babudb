/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.dev;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.dev.transaction.TransactionInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;

/**
 * Interface of {@link DatabaseManager} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */

public interface DatabaseManagerInternal extends DatabaseManager {

    /**
     * Creates a new, empty transaction.
     * <p>
     * Unlike {@link BabuDBInsertGroup}s that are limited to insertions and
     * deletions of entries in a single database, transactions may span multiple
     * databases and include creations and deletions of databases.
     * </p>
     * 
     * @return an empty transaction
     */
    public TransactionInternal createTransaction();
    
    /**
     * Executes a database transaction.
     * <p>
     * Note that the execution is performed synchronously by the invoking thread
     * rather than being enqueued. Thus, it should primarily be used for
     * initialization purposes that take place before the database is accessed.
     * </p>
     * 
     * @param txn
     *            the transaction to execute
     * @throws BabuDBException
     *             if an error occurred while executing the transaction
     */
    public void executeTransaction(TransactionInternal txn) throws BabuDBException;
    
    /**
     * Creates a new database.
     * 
     * @param databaseName
     *            name, must be unique
     * @param numIndices
     *            the number of indices (cannot be changed afterwards)
     * @return the newly created database
     * @throws BabuDBException
     *             if the database directory cannot be created or the config
     *             cannot be saved
     */
    public DatabaseInternal createDatabase(String databaseName, int numIndices) 
            throws BabuDBException;
    
    /**
     * Creates a new database.
     * 
     * @param databaseName
     *            name, must be unique
     * @param numIndices
     *            the number of indices (cannot be changed afterwards)
     * @param comparators
     *            an array of ByteRangeComparators for each index (use only one
     *            instance)
     * @return the newly created database
     * @throws BabuDBException
     *             if the database directory cannot be created or the config
     *             cannot be saved
     */
    public DatabaseInternal createDatabase(String databaseName, int numIndices, 
            ByteRangeComparator[] comparators) throws BabuDBException;
    
    /**
     * Returns the database with the given name.
     * 
     * @param dbName
     *            the database name
     * @return the database
     * @throws BabuDBException
     *             if the database does not exist
     */
    public DatabaseInternal getDatabase(String dbName) throws BabuDBException;
    
    /**
     * Returns the database for the given ID.
     * 
     * @param dbId
     *            the database ID
     * @return the database
     * @throws BabuDBException
     *             if the database does not exist
     */
    public DatabaseInternal getDatabase(int dbId) throws BabuDBException;
    
    /**
     * Returns a map containing all databases.
     * 
     * @return a map containing all databases
     * @throws BabuDBException
     *             if an error occurs
     */
    public Map<String, DatabaseInternal> getDatabasesInternal();
    
    /**
     * @return collection of available {@link Database}.
     */
    public Collection<DatabaseInternal> getDatabaseList();
    
    /**
     * @return a lock to prevent concurrent modification on databases.
     */
    public Object getDBModificationLock();
    
    /**
     * Resets the in-memory database structure.
     * 
     * @throws BabuDBException if this operation fails.
     */
    public void reset() throws BabuDBException;
    
    /**
     * @return the next database identifier to assign.
     */
    public int getNextDBId();
    
    /**
     * Method to manually set the next to use database identifier. 
     * Should only be accessed by DBConfig.
     * 
     * @param id
     */
    public void setNextDBId(int id);
    
    /**
     * @return a map containing all comparators sorted by their class names.
     */
    public Map<String, ByteRangeComparator> getComparatorInstances();
    
    /**
     * Register a database at the DatabaseManger.
     * 
     * @param database
     */
    public void putDatabase(DatabaseInternal database);
    
    /** 
     * @return a set of all database identifiers available.
     */
    public Set<Integer> getAllDatabaseIds();
    
    /**
     * Removes a database given by its id.
     * 
     * @param id
     */
    public void removeDatabaseById(int id);
    
    /**
     * Returns runtime information about the database manager.
     * 
     * @param property
     *            the name of the runtime state property to query
     * @return An object encapsulating certain state information. The type and
     *         data of the object depends on the queried property. If the
     *         property is undefined, <code>null</code> is returned.
     */
    public abstract Object getRuntimeState(String property);
    
    /**
     * Terminates the {@link DatabaseManager}.
     * 
     * @throws BabuDBException if an error occurs.
     */
    public void shutdown() throws BabuDBException;
}
