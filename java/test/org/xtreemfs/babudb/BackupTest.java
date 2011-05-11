/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

/** 
 * @author hoegvist
 */
public class BackupTest extends TestCase {
    
    public static final String  origDir     = "/tmp/babudb_orig/";

    public static final String  backupDir     = "/tmp/babudb_backup/";

    public static final boolean compression = false;
    
    private static final int maxNumRecs = 16;
    
    private static final int maxBlockFileSize = 1024 * 1024 * 512;
    
    private static final int numIndices = 5;
        
    public BackupTest() {
        Logging.start(Logging.LEVEL_DEBUG);
    }
    
    @Before
    public void setUp() throws Exception {
        FSUtils.delTree(new File(origDir));
        FSUtils.delTree(new File(backupDir));
    }
    
    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testConsistentWithSingleDB() throws Exception {
        BabuDB database = BabuDBFactory.createBabuDB(new BabuDBConfig(origDir, origDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, compression, maxNumRecs, maxBlockFileSize));
        
        Database db = database.getDatabaseManager().createDatabase("test", numIndices);
        
        // fill database with random entries
        for (int i = 0; i < numIndices; i++) {
            for (int e = 0; e < 100; e++) {
                int randnum = (int) (Math.random() * Integer.MAX_VALUE);
                final String key = String.valueOf(randnum);
                final String value = "VALUE_" + randnum;
                db.singleInsert(i, key.getBytes(), value.getBytes(),null).get();
            }
        }
        
        System.out.println("Creating backup database...");
        database.getDatabaseManager().dumpAllDatabases(backupDir);
              
        BabuDB babuBackup = BabuDBFactory.createBabuDB(new BabuDBConfig(backupDir, backupDir, 1, 0, 0,
        		SyncMode.SYNC_WRITE, 0, 0, compression, maxNumRecs, maxBlockFileSize));
        
        Database backupDB = babuBackup.getDatabaseManager().getDatabase("test");
        
        // check entries
        for (int i = 0; i < numIndices; i++) {
            System.out.println("checking index " + i);
            Iterator<Entry<byte[], byte[]>> values = db.prefixLookup(i, "1".getBytes(),null).get();
            
            while (values.hasNext()) {
                Entry<byte[], byte[]> e = values.next();
                byte[] v = backupDB.lookup(i, e.getKey(),null).get();
                assertNotNull(v);
                assertEquals(v.length, e.getValue().length);
                for (int p = 0; p < v.length; p++)
                    assertEquals(v[p], e.getValue()[p]);
            }
            System.out.println("checking complete.");
        }

        System.out.println("test inserts in the backup db");
        
        // fill database with random entries
        for (int i = 0; i < numIndices; i++) {
            for (int e = 0; e < 100; e++) {
                int randnum = (int) (Math.random() * Integer.MAX_VALUE);
                final String key = String.valueOf(randnum);
                final String value = "VALUE_" + randnum;
                backupDB.singleInsert(i, key.getBytes(), value.getBytes(),null).get();
            }
        }

        System.out.println("shutting down databases...");
        database.shutdown();
        backupDB.shutdown();
    }

    public static void main(String[] args) {
        TestRunner.run(BackupTest.class);
    }
    
}