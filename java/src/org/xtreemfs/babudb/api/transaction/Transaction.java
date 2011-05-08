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
 * A transaction is a sorted collection of atomically executed operations.
 * Unlike {@link DatabaseInsertGroup}s, transactions may contain insertions
 * across database boundaries, which may include creations and deletions of
 * databases and generation and removal of snapshots.
 * </p>
 * 
 * @author stenjan
 * @author flangner
 */
public interface Transaction {
    
    /**
     * Creates a new snapshot for the database with databaseName using config.
     * 
     * @param databaseName
     * @param config
     * 
     * @return the resulting Transaction.
     */
    public Transaction createSnapshot(String databaseName, SnapshotConfig config);
    
    /**
     * Deletes snapshot with snapshotName of database databaseName.
     * 
     * @param databaseName
     * @param snapshotName
     * 
     * @return the resulting Transaction.
     */
    public Transaction deleteSnapshot(String databaseName, String snapshotName);
    
    /**
     * Creates a new database.
     * 
     * @param databaseName - the database name
     * @param numIndices - the number of indices on the database
     *            
     * @return the resulting Transaction.
     */
    public Transaction createDatabase(String databaseName, int numIndices);
    
    /**
     * Creates a new database.
     * 
     * @param databaseName - the database name
     * @param numIndices - the number of indices on the database
     * @param comparators - an array of comparators for the indices
     *            
     * @return the resulting Transaction.
     */
    public Transaction createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators);
    
    /**
     * Copies database with sourceName to destinationName.
     * 
     * @param sourceName
     * @param destinationName
     * 
     * @return the resulting Transaction.
     */
    public Transaction copyDatabase(String sourceName, String destinationName);
    
    /**
     * Deletes an existing database.
     * 
     * @param databaseName - the name of the database to delete
     *            
     * @return the resulting Transaction.
     */
    public Transaction deleteDatabase(String databaseName);
    
    /**
     * Add a new insert operation to this transaction. Be aware of unpredictable behavior if a 
     * key-value pair of the same database is manipulated twice within the same transaction.
     * 
     * @param databaseName - the name of the database
     * @param indexId - the index in which the key-value pair is inserted.
     * @param key - the key.
     * @param value - the value data.
     *            
     * @return the resulting Transaction.
     */
    public Transaction insertRecord(String databaseName, int indexId, byte[] key, byte[] value);
    
    /**
     * Add a new delete operation to this transaction. Be aware of unpredictable behavior if a 
     * key-value pair of the same database is manipulated twice within the same transaction.
     * 
     * @param databaseName - the name of the database
     * @param indexId - in which the key-value pair is located.
     * @param key - of the key-value pair to delete.
     *            
     * @return the resulting Transaction.
     */
    public Transaction deleteRecord(String databaseName, int indexId, byte[] key);
        
    /**
     * Returns the list of operations contained in the transaction.
     * 
     * @return the list of operations
     */
    public List<Operation> getOperations();  
}
