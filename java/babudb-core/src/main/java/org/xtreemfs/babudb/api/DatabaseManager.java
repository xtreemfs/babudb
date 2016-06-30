/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api;

import java.io.IOException;
import java.util.Map;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.api.transaction.Transaction;
import org.xtreemfs.babudb.api.transaction.TransactionListener;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;

/**
 * Interface to the database manager. The database manager provides methods for
 * management operations, such as creating, deleting and retrieving databases.
 * 
 * @author stenjan
 * 
 */
public interface DatabaseManager {
    
    /**
     * Returns the database with the given name.
     * 
     * @param dbName
     *            the database name
     * @return the database
     * @throws BabuDBException
     *             if the database does not exist
     */
    public Database getDatabase(String dbName) throws BabuDBException;
    
    /**
     * Returns a map containing all databases.
     * 
     * @return a map containing all databases
     */
    public Map<String, Database> getDatabases();
    
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
    public Database createDatabase(String databaseName, int numIndices) throws BabuDBException;
    
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
    public Database createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators)
        throws BabuDBException;
    
    /**
     * Deletes a database.
     * 
     * @param databaseName
     *            name of database to delete
     * @throws BabuDBException
     */
    public void deleteDatabase(String databaseName) throws BabuDBException;
    
    /**
     * Creates a copy of database sourceDB by taking a snapshot, materializing
     * it and loading it as destDB. This does not interrupt operations on
     * sourceDB.
     * 
     * @param sourceDB
     *            the database to copy
     * @param destDB
     *            the new database's name
     * @throws BabuDBException
     */
    public void copyDatabase(String sourceDB, String destDB) throws BabuDBException;
    
    /**
     * Creates a dump (i.e. point-in-time copy) of all databases registered with
     * this DatabaseManager. The dump is stored in the given destination path
     * and is suitable for backup. Creating a dump does not influence the
     * original database. The procedure is as follows:
     * 
     * <ul>
     * <li>Create snapshots of all databases within this BabuDB instance</li>
     * <li>Write out a copy of the database config</li>
     * <li>Materialize the snapshots in the backup directory</li>
     * </ul>
     * 
     * A backup can be recovered by creating a new BabuDB instance configured to
     * use the dump's destination path.
     * 
     * @param destPath
     *            the destination path of the dumped data
     * @throws BabuDBException
     * @throws IOException
     * @throws InterruptedException
     */
    public void dumpAllDatabases(String destPath) throws BabuDBException, IOException, InterruptedException;
    
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
    public Transaction createTransaction();
    
    /**
     * Executes a lightweight database transaction.
     * 
     * @param txn
     *            the transaction to execute
     * @throws BabuDBException
     *             if an error occurred while executing the transaction
     */
    public void executeTransaction(Transaction txn) throws BabuDBException;
    
    /**
     * Adds a new transaction listener. The listener is notified after the
     * execution of a transaction.
     * 
     * @param listener
     *            the listener to add
     */
    public void addTransactionListener(TransactionListener listener);
    
    /**
     * Removes a transaction listener.
     * 
     * @param listener
     *            the listener to remove
     */
    public void removeTransactionListener(TransactionListener listener);
    
}
