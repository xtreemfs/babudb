/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.api.transaction;

import java.util.List;

import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;

/**
 * A lightweight BabuDB transaction.
 * <p>
 * A transaction is a sorted collection of atomically executed operations.
 * Unlike {@link DatabaseInsertGroup}s, transactions may contain insertions
 * across database boundaries, which may include creations and deletions of
 * databases.
 * </p>
 * 
 * @author stenjan
 * 
 */
public interface Transaction {
    
    /**
     * Creates a new database.
     * 
     * @param databaseName
     *            the database name
     * @param numIndices
     *            the number of indices on the database
     */
    public void createDatabase(String databaseName, int numIndices);
    
    /**
     * Creates a new database.
     * 
     * @param databaseName
     *            the database name
     * @param numIndices
     *            the number of indices on the database
     * @param comparators
     *            an array of comparators for the indices
     */
    public void createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators);
    
    /**
     * Deletes an existing database.
     * 
     * @param databaseName
     *            the name of the database to delete
     */
    public void deleteDatabase(String databaseName);
    
    /**
     * Add a new insert operation to this transaction.
     * 
     * @param indexId
     *            - the index in which the key-value pair is inserted.
     * @param key
     *            - the key.
     * @param value
     *            - the value data.
     */
    public void insertRecord(String databaseName, int indexId, byte[] key, byte[] value);
    
    /**
     * Add a new delete operation to this transaction.
     * 
     * @param indexId
     *            - in which the key-value pair is located.
     * @param key
     *            - of the key-value pair to delete.
     */
    public void deleteRecord(String databaseName, int indexId, byte[] key);
    
    /**
     * Returns the list of operations contained in the transaction.
     * 
     * @return the lsit of operations
     */
    public List<Operation> getOperations();
    
}
