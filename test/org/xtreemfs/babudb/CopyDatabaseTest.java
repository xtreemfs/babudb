/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb;

import java.util.Iterator;
import java.util.Map.Entry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.common.logging.Logging;
import static org.junit.Assert.*;

/**
 *
 * @author bjko
 */
public class CopyDatabaseTest {

    public static final String baseDir = "/tmp/lsmdb-test/";

    private BabuDB database;
    
    public CopyDatabaseTest() {
        Logging.start(Logging.LEVEL_DEBUG);
    }

    @Before
    public void setUp() throws Exception {
        Process p = Runtime.getRuntime().exec("rm -rf "+baseDir);
        p.waitFor();
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testCopyDatabase() throws Exception {
        
        final int NUMIDX = 5;
        
        database = new BabuDB(baseDir,baseDir,1,0,0,SyncMode.ASYNC,0,0);
        
        database.createDatabase("testDB", NUMIDX);
        
        //fill database with random entries
        for (int i = 0; i < NUMIDX; i++) {
            for (int e = 0; e < 100; e++) {
                int randnum = (int)(Math.random()*Integer.MAX_VALUE);
                final String key = String.valueOf(randnum);
                final String value = "VALUE_"+randnum;
                database.syncSingleInsert("testDB", i, key.getBytes(), value.getBytes());
            }
        }
        
        System.out.println("starting copy");
        //copy database
        database.copyDatabase("testDB", "copyDB", null, null);
        
        System.out.println("copy complete");
        //check entries
        for (int i = 0; i < NUMIDX; i++) {
            System.out.println("checking index "+i);
            Iterator<Entry<byte[],byte[]>> values = database.syncPrefixLookup("testDB", i, "1".getBytes());
            
            while (values.hasNext()) {
                Entry<byte[],byte[]> e = values.next();
                byte[] v = database.syncLookup("copyDB", i, e.getKey());
                assertNotNull(v);
                assertEquals(v.length, e.getValue().length);
                for (int p = 0; p < v.length; p++)
                    assertEquals(v[p], e.getValue()[p]);
            }
            System.out.println("checking complete.");
        }
    }

}