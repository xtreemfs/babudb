/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
 * 
 * @author bjko
 */
public class CopyDatabaseTest extends TestCase {
    
    public static final String baseDir = "/tmp/lsmdb-test/";
    
    public static final boolean compression = false;
    
    private static final int maxNumRecs = 16;
    
    private static final int maxBlockFileSize = 1024 * 1024 * 512;
    
    private BabuDB             database;
    
    public CopyDatabaseTest() {
        Logging.start(Logging.LEVEL_DEBUG);
    }
    
    @Before
    public void setUp() throws Exception {
        FSUtils.delTree(new File(baseDir));
    }
    
    @After
    public void tearDown() throws Exception {
        database.shutdown();
    }
    
    @Test
    public void testCopyDatabase() throws Exception {
        
        final int NUMIDX = 5;
        
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0, SyncMode.ASYNC, 0,
            0, compression, maxNumRecs, maxBlockFileSize),null);
        
        Database db = database.getDatabaseManager().createDatabase("testDB", NUMIDX);
        
        // fill database with random entries
        for (int i = 0; i < NUMIDX; i++) {
            for (int e = 0; e < 100; e++) {
                int randnum = (int) (Math.random() * Integer.MAX_VALUE);
                final String key = String.valueOf(randnum);
                final String value = "VALUE_" + randnum;
                db.singleInsert(i, key.getBytes(), value.getBytes(),null).get();
            }
        }
        
        System.out.println("starting copy");
        // copy database
        database.getDatabaseManager().copyDatabase("testDB", "copyDB");
        
        System.out.println("copy complete");
        // check entries
        for (int i = 0; i < NUMIDX; i++) {
            System.out.println("checking index " + i);
            Iterator<Entry<byte[], byte[]>> values = db.prefixLookup(i, "1".getBytes(),null).get();
            
            while (values.hasNext()) {
                Entry<byte[], byte[]> e = values.next();
                byte[] v = database.getDatabaseManager().getDatabase("copyDB").lookup(i, e.getKey(),null).get();
                assertNotNull(v);
                assertEquals(v.length, e.getValue().length);
                for (int p = 0; p < v.length; p++)
                    assertEquals(v[p], e.getValue()[p]);
            }
            System.out.println("checking complete.");
        }
    }
    
    public static void main(String[] args) {
        TestRunner.run(CopyDatabaseTest.class);
    }
    
}