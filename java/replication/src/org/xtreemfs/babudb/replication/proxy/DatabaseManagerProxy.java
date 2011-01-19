/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
 */
package org.xtreemfs.babudb.replication.proxy;

import java.io.IOException;
import java.util.Map;

import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.replication.policy.Policy;

/**
 * Stub to redirect Database read-only requests to a remote master if necessary.
 * 
 * @see Policy
 * 
 * @author flangner
 * @since 01/19/2011
 */
class DatabaseManagerProxy implements DatabaseManager {

    private final DatabaseManager localDBMan;

    public DatabaseManagerProxy(DatabaseManager localDBMan) {
        this.localDBMan = localDBMan;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabase(java.lang.String)
     */
    @Override
    public Database getDatabase(String dbName) throws BabuDBException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#getDatabases()
     */
    @Override
    public Map<String, Database> getDatabases() throws BabuDBException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(java.lang.String, int)
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices) throws BabuDBException {
        return localDBMan.createDatabase(databaseName, numIndices);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#createDatabase(java.lang.String, int, org.xtreemfs.babudb.api.index.ByteRangeComparator[])
     */
    @Override
    public Database createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators)
            throws BabuDBException {
        return localDBMan.createDatabase(databaseName, numIndices, comparators);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#deleteDatabase(java.lang.String)
     */
    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {
        localDBMan.deleteDatabase(databaseName);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#copyDatabase(java.lang.String, java.lang.String)
     */
    @Override
    public void copyDatabase(String sourceDB, String destDB) throws BabuDBException {
        localDBMan.copyDatabase(sourceDB, destDB);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.DatabaseManager#dumpAllDatabases(java.lang.String)
     */
    @Override
    public void dumpAllDatabases(String destPath) throws BabuDBException, IOException, InterruptedException {
        localDBMan.dumpAllDatabases(destPath);
    }

}
