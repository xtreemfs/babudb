/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api.transaction;

import java.util.List;

import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;

/**
 * A lightweight BabuDB transaction.
 * <p>
 * A transaction is a sorted collection of modifications that is executed
 * atomically. Transactions resemble {@link DatabaseInsertGroup}s, but they may
 * contain modifications across multiple databases, creations and deletions of
 * databases as well as snapshot operations.
 * </p>
 * <p>
 * BabuDB transactions are called lightweight, as their semantics differ from
 * the ones typically provided by database systems. The most important
 * differences are:
 * </p>
 * <ul>
 * <li>BabuDB transactions may only contain modifications, no lookups.</li>
 * <li>
 * In the event of an error (which may e.g. be caused by an operation that
 * attempts to create or delete non-existing database or to insert a record in a
 * non-existing index), all operations are executed until the one that caused
 * the error. This means that no roll-back will be performed for operations that
 * have been executed before. However, all operations prior to the failed one
 * will be executed in an atomic, non-interruptible fashion.</li>
 * 
 * @author stenjan
 * @author flangner
 */
public interface Transaction {
    
    /**
     * Creates a new snapshot.
     * 
     * @param databaseName
     *            the database name
     * @param config
     *            the snapshot configuration
     * 
     * @return a reference to the transaction
     */
    public Transaction createSnapshot(String databaseName, SnapshotConfig config);
    
    /**
     * Deletes a snapshot.
     * 
     * @param databaseName
     *            the database name
     * @param snapshotName
     *            the snapshot name
     * 
     * @return a reference to the transaction
     */
    public Transaction deleteSnapshot(String databaseName, String snapshotName);
    
    /**
     * Creates a new database.
     * 
     * @param databaseName
     *            the database name
     * @param numIndices
     *            the number of indices on the database
     * 
     * @return a reference to the transaction
     */
    public Transaction createDatabase(String databaseName, int numIndices);
    
    /**
     * Creates a new database.
     * 
     * @param databaseName
     *            the database name
     * @param numIndices
     *            the number of indices on the database
     * @param comparators
     *            an array of comparators for the indices
     * 
     * @return a reference to the transaction
     */
    public Transaction createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators);
    
    /**
     * Copies a database.
     * 
     * @param sourceName
     *            the name of the source database
     * @param destinationName
     *            the name of the destination database
     * 
     * @return a reference to the transaction
     */
    public Transaction copyDatabase(String sourceName, String destinationName);
    
    /**
     * Deletes an existing database.
     * 
     * @param databaseName
     *            the name of the database to delete
     * 
     * @return a reference to the transaction
     */
    public Transaction deleteDatabase(String databaseName);
    
    /**
     * Inserts a key-value pair.
     * 
     * @param databaseName
     *            the name of the database
     * @param indexId
     *            the index in which the key-value pair is inserted
     * @param key
     *            the key
     * @param value
     *            the value
     * 
     * @return a reference to the transaction
     */
    public Transaction insertRecord(String databaseName, int indexId, byte[] key, byte[] value);
    
    /**
     * Deletes a key-value pair.
     * 
     * @param databaseName
     *            the name of the database
     * @param indexId
     *            in which the key-value pair is located
     * @param key
     *            the key of the record to delete
     * 
     * @return a reference to the transaction
     */
    public Transaction deleteRecord(String databaseName, int indexId, byte[] key);
    
    /**
     * Returns the list of operations contained in the transaction.
     * 
     * @return the list of operations
     */
    public List<Operation> getOperations();
}
