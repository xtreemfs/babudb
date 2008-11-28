/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.common.logging.Logging;

import static org.junit.Assert.*;

/**
 *
 * @author bjko
 */
public class BabuDBTest {
    
    public static final String baseDir = "/tmp/lsmdb-test/";

    private BabuDB database;
    
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
        database = new BabuDB(baseDir,baseDir,1,0,0,false);
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
        
        database = new BabuDB(baseDir,baseDir,2,0,0,false);
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
        database = new BabuDB(baseDir,baseDir,1,0,0,false);
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
                
        
        database = new BabuDB(baseDir,baseDir,2,0,0,false);
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
        database = new BabuDB(baseDir,baseDir,1,0,0,false);
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
                
        
        database = new BabuDB(baseDir,baseDir,2,0,0,false);
        
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
}