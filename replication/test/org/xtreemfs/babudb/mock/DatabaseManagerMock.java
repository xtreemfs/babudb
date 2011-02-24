/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;

/**
 * 
 * @author flangner
 * @since 02/23/2011
 */

public class DatabaseManagerMock implements DatabaseManager {

    private final Map<String, Database> dbs = new HashMap<String, Database>();
    
    @Override
    public void copyDatabase(String sourceDB, String destDB)
            throws BabuDBException {
        
        if (dbs.containsKey(sourceDB)) {
            dbs.put(destDB, dbs.get(sourceDB));
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-copy failed!");
        }
    }

    @Override
    public Database createDatabase(String databaseName, int numIndices)
            throws BabuDBException {
        
        return createDatabase(databaseName, numIndices, null);
    }

    @Override
    public Database createDatabase(String databaseName, int numIndices,
            ByteRangeComparator[] comparators) throws BabuDBException {
        
        if (!dbs.containsKey(databaseName)) {
            Database result = new DatabaseMock(databaseName, numIndices, comparators);
            dbs.put(databaseName, result);
            return result;
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-create failed!");
        }
    }

    @Override
    public void deleteDatabase(String databaseName) throws BabuDBException {
        
        if (dbs.containsKey(databaseName)) {
            dbs.remove(databaseName);
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
    public Database getDatabase(String dbName) throws BabuDBException {

        if (dbs.containsKey(dbName)) {
            return dbs.get(dbName);
        } else {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, "Test-getDB failed!");
        }
    }

    @Override
    public Map<String, Database> getDatabases() throws BabuDBException {
        return dbs;
    }

}
