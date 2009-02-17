/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import java.util.Iterator;
import java.util.Map.Entry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDBFactory.BabuDBImpl;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.LSMLookupInterface;
import org.xtreemfs.common.logging.Logging;

import static org.junit.Assert.*;

/**
 *
 * @author bjko
 */
public class BabuDBTest {
    
    public static final String baseDir = "/tmp/lsmdb-test/";

    private BabuDBImpl database;
    
    public BabuDBTest() {
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

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
    @Test
    public void testReplayAfterCrash() throws Exception {
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0);
        database.createDatabase("test", 2);
        database.syncSingleInsert("test", 0, "Yagga".getBytes(), "Brabbel".getBytes());
        database.checkpoint();
        byte[] result = database.syncLookup("test", 0, "Yagga".getBytes());
        String value = new String(result);
        assertEquals(value, "Brabbel");
        
        database.syncSingleInsert("test", 0, "Brabbel".getBytes(), "Blupp".getBytes());
        result = database.syncLookup("test", 0, "Brabbel".getBytes());
        value = new String(result);
        assertEquals(value, "Blupp");
        
        database.syncSingleInsert("test", 0, "Blupp".getBytes(), "Blahh".getBytes());
        result = database.syncLookup("test", 0, "Blupp".getBytes());
        value = new String(result);
        assertEquals(value, "Blahh");
        
        
        database.__test_killDB_dangerous();
        Thread.sleep(500);
        
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,2,0,0,SyncMode.SYNC_WRITE,0,0);
        result = database.syncLookup("test", 0, "Yagga".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Brabbel");
        
        System.out.println("");
        
        result = database.syncLookup("test", 0, "Brabbel".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blupp");
        
        result = database.syncLookup("test", 0, "Blupp".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blahh");
        
        System.out.println("shutting down database...");
        
        database.shutdown();
    }
    
    @Test
    public void testShutdownAfterCheckpoint() throws Exception {
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0);
        database.createDatabase("test", 2);
        database.syncSingleInsert("test", 0, "Yagga".getBytes(), "Brabbel".getBytes());
        
        byte[] result = database.syncLookup("test", 0, "Yagga".getBytes());
        String value = new String(result);
        assertEquals(value, "Brabbel");
        
        database.syncSingleInsert("test", 0, "Brabbel".getBytes(), "Blupp".getBytes());
        result = database.syncLookup("test", 0, "Brabbel".getBytes());
        value = new String(result);
        assertEquals(value, "Blupp");
        
        database.syncSingleInsert("test", 0, "Blupp".getBytes(), "Blahh".getBytes());
        result = database.syncLookup("test", 0, "Blupp".getBytes());
        value = new String(result);
        assertEquals(value, "Blahh");
        
        
        database.checkpoint();
        database.shutdown();
                
        
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,2,0,0,SyncMode.SYNC_WRITE,0,0);
        result = database.syncLookup("test", 0, "Yagga".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Brabbel");
        
        System.out.println("");
        
        result = database.syncLookup("test", 0, "Brabbel".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blupp");
        
        result = database.syncLookup("test", 0, "Blupp".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blahh");
        
        System.out.println("shutting down database...");
        
        database.shutdown();
    }
    
    @Test
    public void testMultipleIndices() throws Exception {
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0);
        database.createDatabase("test", 3);
        
        BabuDBInsertGroup ir = database.createInsertGroup("test");
        ir.addInsert(0, "Key1".getBytes(),
                "Value1".getBytes());
        ir.addInsert(1, "Key2".getBytes(),
                "Value2".getBytes());
        ir.addInsert(2, "Key3".getBytes(),
                "Value3".getBytes());
        database.syncInsert(ir);
        
        
        database.checkpoint();
        database.shutdown();
                
        
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,2,0,0,SyncMode.SYNC_WRITE,0,0);
        
        byte[] result = database.syncLookup("test", 0, "Key1".getBytes());
        assertNotNull(result);
        String value = new String(result);
        assertEquals(value, "Value1");
        
        System.out.println("");
        
        result = database.syncLookup("test", 1, "Key2".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value2");
        
        result = database.syncLookup("test", 2, "Key3".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value3");

        System.out.println("shutting down database...");
        
        database.shutdown();
    }

    @Test
    public void testMultipleIndicesAndCheckpoint() throws Exception {
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0);
        database.createDatabase("test", 4);

        BabuDBInsertGroup ir = database.createInsertGroup("test");
        ir.addInsert(0, "Key1".getBytes(),
                "Value1".getBytes());
        ir.addInsert(1, "Key2".getBytes(),
                "Value2".getBytes());
        ir.addInsert(2, "Key3".getBytes(),
                "Value3".getBytes());
        database.syncInsert(ir);


        database.checkpoint();

        byte[] result = database.syncLookup("test", 0, "Key1".getBytes());
        assertNotNull(result);
        String value = new String(result);
        assertEquals(value, "Value1");

        System.out.println("");

        result = database.syncLookup("test", 1, "Key2".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value2");

        result = database.syncLookup("test", 2, "Key3".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value3");

        Iterator<Entry<byte[],byte[]>> iter = database.syncPrefixLookup("test", 3, "Key3".getBytes());
        assertNotNull(iter);

        

        System.out.println("shutting down database...");

        database.shutdown();
    }
    
    @Test
    public void testUserDefinedLookup() throws Exception {
        
        database = (BabuDBImpl) BabuDBFactory.getBabuDB(baseDir,baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0);
        database.createDatabase("test", 3);
        
        BabuDBInsertGroup ir = database.createInsertGroup("test");
        ir.addInsert(0, "Key1".getBytes(),
                "Value1".getBytes());
        ir.addInsert(1, "Key2".getBytes(),
                "Value2".getBytes());
        ir.addInsert(2, "Key3".getBytes(),
                "Value3".getBytes());
        database.syncInsert(ir);
        
        UserDefinedLookup lookup = new UserDefinedLookup() {

            public Object execute(LSMLookupInterface database) throws BabuDBException {              
                if ( (database.lookup(0, "Key1".getBytes()) != null) &&
                     (database.lookup(1, "Key2".getBytes()) != null)) {
                    return new Boolean(true);
                } else {
                    return new Boolean(false);
                }
            }
        };
        
        Boolean result = (Boolean)database.syncUserDefinedLookup("test", lookup);
        assertTrue(result);
        
        
        System.out.println("shutting down database...");
        
        database.shutdown();
        
        
        
    }
}