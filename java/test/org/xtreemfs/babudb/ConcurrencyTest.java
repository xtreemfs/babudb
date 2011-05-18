/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * 
 * @author stenjan
 *
 */
public class ConcurrencyTest extends TestCase {
    
    public static final String  baseDir          = "/tmp/lsmdb-test/";
    
    public static final boolean compression      = false;
    
    private static final int    maxNumRecs       = 16;
    
    private static final int    maxBlockFileSize = 1024 * 1024 * 512;
    
    private BabuDB              database;
    
    public ConcurrencyTest() {
        Logging.start(Logging.LEVEL_ERROR);
    }
    
    @Before
    public void setUp() throws Exception {
        FSUtils.delTree(new File(baseDir));
        
        System.out.println("=== " + getName() + " ===");
    }
    
    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testLargeIndexWithConcurrentCheckpoint() throws Exception {
        
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 0, 1000, 0,
            SyncMode.ASYNC, 0, 0, compression, maxNumRecs, maxBlockFileSize, true, 1000, 7));
        
        Database db = database.getDatabaseManager().createDatabase("test", 1);
        
        final long entryCount = 50000;
        
        Thread cpThread = new Thread() {
            public void run() {
                try {
                    for (int i = 0; i < 2; i++) {
                        database.getCheckpointer().checkpoint();
                        sleep(1000);
                    }
                } catch (BabuDBException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        
        for (long i = 0; i < entryCount; i++) {
            
            ByteBuffer keyBuf = ByteBuffer.wrap(new byte[8]);
            keyBuf.putLong(0, i);
            
            ByteBuffer valBuf = ByteBuffer.wrap(new byte[32]);
            valBuf.putLong(0, i);
            valBuf.putLong(24, i);
            
            if (i == 10) {
                cpThread.start();
                Thread.sleep(2);
            }
            
            db.singleInsert(0, keyBuf.array(), valBuf.array(), null).get();
        }
        
        ByteBuffer keyBuf = ByteBuffer.wrap(new byte[8]);
        ByteBuffer valBuf = ByteBuffer.wrap(new byte[32]);
        for (long i = 0; i < entryCount; i++) {
            
            keyBuf.putLong(0, i);
            valBuf.putLong(0, i);
            valBuf.putLong(24, i);
            
            byte[] val = db.lookup(0, keyBuf.array(), null).get();
            assertEquals(valBuf.array(), val);
        }
        
        Iterator<Entry<byte[], byte[]>> it = db.prefixLookup(0, new byte[0], null).get();
        for (long i = 0; i < entryCount; i++) {
            
            Entry<byte[], byte[]> next = it.next();
            
            byte[] key = next.getKey();
            byte[] val = next.getValue();
            
            long number = ByteBuffer.wrap(key).getLong(0);
            valBuf.putLong(0, number);
            valBuf.putLong(24, number);
            
            assertEquals(val, valBuf.array());
        }
        
        assertFalse(it.hasNext());
        
        cpThread.join();
        database.shutdown();
        
        System.out.println(BufferPool.getStatus());
        
    }
    
    private void assertEquals(byte[] b1, byte[] b2) {
        assertEquals(b1.length, b2.length);
        for (int i = 0; i < b1.length; i++)
            assertEquals(b1[i], b2[i]);
    }
    
    public static void main(String[] args) {
        TestRunner.run(ConcurrencyTest.class);
    }
    
}
