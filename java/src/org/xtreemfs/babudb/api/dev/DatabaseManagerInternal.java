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

import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;

/**
 * Interface of {@link DatabaseManager} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @since 03/18/2011
 */

public interface DatabaseManagerInternal extends DatabaseManager {

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
    public DatabaseInternal getDatabase(int dbId);
    
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
     * Terminates the {@link DatabaseManager}.
     * 
     * @throws BabuDBException if an error occurs.
     */
    public void shutdown() throws BabuDBException;
}
