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

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.LSMLookupInterface;
import org.xtreemfs.include.common.config.BabuDBConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 * 
 * @author bjko
 */
public class BabuDBTest extends TestCase {
    
    public static final String baseDir = "/tmp/lsmdb-test/";
    
    private BabuDB         database;
    
    public BabuDBTest() {
        Logging.start(Logging.LEVEL_DEBUG);
    }
    
    @Before
    public void setUp() throws Exception {
        Process p = Runtime.getRuntime().exec("rm -rf " + baseDir);
        p.waitFor();
        
    }
    
    @After
    public void tearDown() throws Exception {
    }
    
    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
    @Test
    public void testReplayAfterCrash() throws Exception {
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
                SyncMode.SYNC_WRITE, 0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 2);
        db.syncSingleInsert(0, "Yagga".getBytes(), "Brabbel".getBytes());
        database.getCheckpointer().checkpoint();
        byte[] result = db.syncLookup(0, "Yagga".getBytes());
        String value = new String(result);
        assertEquals(value, "Brabbel");
        
        db.syncSingleInsert(0, "Brabbel".getBytes(), "Blupp".getBytes());
        result = db.syncLookup(0, "Brabbel".getBytes());
        value = new String(result);
        assertEquals(value, "Blupp");
        
        db.syncSingleInsert(0, "Blupp".getBytes(), "Blahh".getBytes());
        result = db.syncLookup(0, "Blupp".getBytes());
        value = new String(result);
        assertEquals(value, "Blahh");
        
        database.__test_killDB_dangerous();
        Thread.sleep(500);
        
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 2, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        db = database.getDatabaseManager().getDatabase("test");
        result = db.syncLookup(0, "Yagga".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Brabbel");
                
        result = db.syncLookup(0, "Brabbel".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blupp");
        
        result = db.syncLookup(0, "Blupp".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blahh");
        
        System.out.println("shutting down database...");
        
        database.shutdown();
    }
    
    @Test
    public void testShutdownAfterCheckpoint() throws Exception {
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 2);
        db.syncSingleInsert(0, "Yagga".getBytes(), "Brabbel".getBytes());
        
        byte[] result = db.syncLookup(0, "Yagga".getBytes());
        String value = new String(result);
        assertEquals(value, "Brabbel");
        
        db.syncSingleInsert(0, "Brabbel".getBytes(), "Blupp".getBytes());
        result = db.syncLookup(0, "Brabbel".getBytes());
        value = new String(result);
        assertEquals(value, "Blupp");
        
        db.syncSingleInsert(0, "Blupp".getBytes(), "Blahh".getBytes());
        result = db.syncLookup(0, "Blupp".getBytes());
        value = new String(result);
        assertEquals(value, "Blahh");
        
        database.getCheckpointer().checkpoint();
        database.shutdown();
        
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 2, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        db = database.getDatabaseManager().getDatabase("test");
        result = db.syncLookup(0, "Yagga".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Brabbel");
        
        result = db.syncLookup(0, "Brabbel".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blupp");
        
        result = db.syncLookup(0, "Blupp".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Blahh");
        
        System.out.println("shutting down database...");
        
        database.shutdown();
    }
    
    @Test
    public void testMultipleIndices() throws Exception {
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 3);
        
        BabuDBInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
        ir.addInsert(1, "Key2".getBytes(), "Value2".getBytes());
        ir.addInsert(2, "Key3".getBytes(), "Value3".getBytes());
        db.syncInsert(ir);
        
        database.getCheckpointer().checkpoint();
        database.shutdown();
        
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 2, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        db = database.getDatabaseManager().getDatabase("test");
        
        byte[] result = db.syncLookup(0, "Key1".getBytes());
        assertNotNull(result);
        String value = new String(result);
        assertEquals(value, "Value1");
                
        result = db.syncLookup(1, "Key2".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value2");
        
        result = db.syncLookup(2, "Key3".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value3");
        
        System.out.println("shutting down database...");
        
        database.shutdown();
    }
    
    @Test
    public void testMultipleIndicesAndCheckpoint() throws Exception {
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 4);
        
        BabuDBInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
        ir.addInsert(1, "Key2".getBytes(), "Value2".getBytes());
        ir.addInsert(2, "Key3".getBytes(), "Value3".getBytes());
        db.syncInsert(ir);
        
        database.getCheckpointer().checkpoint();
        
        byte[] result = db.syncLookup(0, "Key1".getBytes());
        assertNotNull(result);
        String value = new String(result);
        assertEquals(value, "Value1");
        
        result = db.syncLookup(1, "Key2".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value2");
        
        result = db.syncLookup(2, "Key3".getBytes());
        assertNotNull(result);
        value = new String(result);
        assertEquals(value, "Value3");
        
        Iterator<Entry<byte[], byte[]>> iter = db.syncPrefixLookup(3, "Key3"
                .getBytes());
        assertNotNull(iter);
        ir = db.createInsertGroup();
        ir.addDelete(0, "Key1".getBytes());
        ir.addInsert(1, "Key2".getBytes(),
                "Value2.2".getBytes());
        ir.addInsert(2, "Key3".getBytes(),
                "Value2.3".getBytes());
        db.syncInsert(ir);
        database.getCheckpointer().checkpoint();

        iter = db.syncPrefixLookup(0, "Key3".getBytes());
        assertNotNull(iter);

        System.out.println("shutting down database...");
        
        database.shutdown();
    }
    
    @Test
    public void testUserDefinedLookup() throws Exception {
        
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 3);
        
        BabuDBInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
        ir.addInsert(1, "Key2".getBytes(), "Value2".getBytes());
        ir.addInsert(2, "Key3".getBytes(), "Value3".getBytes());
        db.syncInsert(ir);
        
        UserDefinedLookup lookup = new UserDefinedLookup() {
            
            public Object execute(LSMLookupInterface database) throws BabuDBException {
                if ((database.lookup(0, "Key1".getBytes()) != null)
                    && (database.lookup(1, "Key2".getBytes()) != null)) {
                    return new Boolean(true);
                } else {
                    return new Boolean(false);
                }
            }
        };
        
        Boolean result = (Boolean) db.syncUserDefinedLookup(lookup);
        assertTrue(result);
        
        System.out.println("shutting down database...");
        
        database.shutdown();
        
    }
    
    @Test
    public void testDirectAccess() throws Exception {
        
        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0, SyncMode.ASYNC,
            0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 2);
        
        for (int i = 0; i < 100000; i++) {
            BabuDBInsertGroup ir = db.createInsertGroup();
            ir.addInsert(0, (i + "").getBytes(), "bla".getBytes());
            ir.addInsert(1, (i + "").getBytes(), "bla".getBytes());
            db.directInsert(ir);
        }
        
        database.getCheckpointer().checkpoint();
        
        for (int i = 0; i < 100000; i++) {
            
            byte[] v0 = db.directLookup(0, (i + "").getBytes());
            byte[] v1 = db.directLookup(1, (i + "").getBytes());
            
            assertEquals("bla", new String(v0));
            assertEquals("bla", new String(v1));
        }
    }

    @Test
    public void testInsDelGet() throws Exception {

        database = (BabuDB) BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0, SyncMode.ASYNC,
            0, 0));
        Database db = database.getDatabaseManager().createDatabase("test", 3);

        for (int i = 0; i < 1000; i++) {
            BabuDBInsertGroup ir = db.createInsertGroup();
            ir.addInsert(0, (i + "").getBytes(), "bla".getBytes());
            ir.addInsert(1, (i + "").getBytes(), "bla".getBytes());
            ir.addInsert(2, (i + "").getBytes(), "bla".getBytes());
            db.directInsert(ir);
        }

        byte[] data = new byte[2048];
        for (int i = 0; i < 1000; i++) {
            BabuDBInsertGroup ir = db.createInsertGroup();
            ir.addInsert(0, (i + "").getBytes(), data);
            ir.addInsert(1, (i + "").getBytes(), data);
            ir.addInsert(2, (i + "").getBytes(), data);
            db.directInsert(ir);
        }

        database.getCheckpointer().checkpoint();

        for (int i = 0; i < 1000; i++) {

            byte[] v0 = db.directLookup(0, (i + "").getBytes());
            byte[] v1 = db.directLookup(1, (i + "").getBytes());
            byte[] v2 = db.directLookup(2, (i + "").getBytes());

            assertNotNull(v0);
            assertNotNull(v1);
            assertNotNull(v2);
        }

        for (int i = 0; i < 1000; i++) {
            BabuDBInsertGroup ir = db.createInsertGroup();
            ir.addDelete(0, (i + "").getBytes());
            ir.addDelete(1, (i + "").getBytes());
            ir.addDelete(2, (i + "").getBytes());
            db.directInsert(ir);
        }

        

        for (int i = 0; i < 1000; i++) {

            byte[] v0 = db.directLookup(0, (i + "").getBytes());
            byte[] v1 = db.directLookup(1, (i + "").getBytes());
            byte[] v2 = db.directLookup(2, (i + "").getBytes());

            assertNull(v0);
            assertNull(v1);
            assertNull(v2);
        }
    }
    
    public static void main(String[] args) {
        TestRunner.run(BabuDBTest.class);
    }
    
}