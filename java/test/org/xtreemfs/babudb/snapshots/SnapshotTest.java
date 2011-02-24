/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.snapshots;

import java.io.File;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.Before;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRO;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * 
 * @author bjko
 */
public class SnapshotTest extends TestCase {
    
    public static final String  baseDir          = "/tmp/lsmdb-test/";
    
    public static final int     DEBUG_LEVEL      = Logging.LEVEL_ERROR;
    
    public static final boolean COMPRESSION      = false;
    
    public static final boolean MMAP             = false;
    
    private static final int    maxNumRecs       = 16;
    
    private static final int    maxBlockFileSize = 1024 * 1024 * 512;
    
    private BabuDB              database;
    
    public SnapshotTest() {
        Logging.start(DEBUG_LEVEL);
    }
    
    @Before
    public void setUp() throws Exception {
        FSUtils.delTree(new File(baseDir));
    }
    
    @After
    public void tearDown() throws Exception {
        if (database != null)
            database.shutdown();
    }
    
    public void testSimpleSnapshot() throws Exception {
        
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 0, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        Database db = database.getDatabaseManager().createDatabase("test", 3);
        
        // add some key-value pairs
        DatabaseInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "Key1".getBytes(), "Value1".getBytes());
        ir.addInsert(0, "Key2".getBytes(), "Value2".getBytes());
        ir.addInsert(0, "Key3".getBytes(), "Value3".getBytes());
        db.insert(ir, null).get();
        
        // create a snapshot (in memory)
        database.getSnapshotManager().createPersistentSnapshot("test",
            new DefaultSnapshotConfig("snap1", new int[] { 0, 1 }, null, null));
        DatabaseRO snap1 = database.getSnapshotManager().getSnapshotDB("test", "snap1");
        
        // overwrite original values
        ir = db.createInsertGroup();
        ir.addInsert(0, "Key1".getBytes(), "x".getBytes());
        ir.addInsert(0, "Key2".getBytes(), "x".getBytes());
        ir.addInsert(0, "Key3".getBytes(), "x".getBytes());
        ir.addInsert(1, "bla".getBytes(), "blub".getBytes());
        db.insert(ir, null).get();
        
        // check if the snapshot still contains the old values
        for (int i = 1; i < 4; i++)
            assertEquals("Value" + i, new String(snap1.lookup(0, ("Key" + i).getBytes(), null).get()));
        
        Iterator<Entry<byte[], byte[]>> it = snap1.prefixLookup(0, "Key".getBytes(), null).get();
        for (int i = 1; i < 4; i++) {
            Entry<byte[], byte[]> next = it.next();
            assertEquals("Key" + i, new String(next.getKey()));
            assertEquals("Value" + i, new String(next.getValue()));
        }
        
        assertNull(snap1.lookup(1, "bla".getBytes(), null).get());
        
        // create a checkpoint
        database.getCheckpointer().checkpoint();
        
        // create a second snapshot
        database.getSnapshotManager().createPersistentSnapshot("test",
            new DefaultSnapshotConfig("snap2", new int[] { 0, 1 }, null, null));
        DatabaseRO snap2 = database.getSnapshotManager().getSnapshotDB("test", "snap2");
        
        // check whether both the first snapshot and the second snapshot contain
        // the correct values
        for (int i = 1; i < 4; i++)
            assertEquals("Value" + i, new String(snap1.lookup(0, ("Key" + i).getBytes(), null).get()));
        
        for (int i = 1; i < 4; i++)
            assertEquals("x", new String(snap2.lookup(0, ("Key" + i).getBytes(), null).get()));
        
        it = snap1.prefixLookup(0, "Key".getBytes(), null).get();
        for (int i = 1; i < 4; i++) {
            Entry<byte[], byte[]> next = it.next();
            assertEquals("Key" + i, new String(next.getKey()));
            assertEquals("Value" + i, new String(next.getValue()));
        }
        
        assertNull(snap1.lookup(1, "bla".getBytes(), null).get());
        
        it = snap2.prefixLookup(0, "Key".getBytes(), null).get();
        for (int i = 1; i < 4; i++) {
            Entry<byte[], byte[]> next = it.next();
            assertEquals("Key" + i, new String(next.getKey()));
            assertEquals("x", new String(next.getValue()));
        }
        
        assertEquals("blub", new String(snap2.lookup(1, "bla".getBytes(), null).get()));
        
    }
    
    public void testPartialSnapshot() throws Exception {
        
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        Database db = database.getDatabaseManager().createDatabase("test", 4);
        
        // add some key-value pairs
        DatabaseInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "testxyz".getBytes(), "v1".getBytes());
        ir.addInsert(0, "test".getBytes(), "v2".getBytes());
        ir.addInsert(0, "testabc".getBytes(), "v3".getBytes());
        ir.addInsert(0, "yagga".getBytes(), "bla".getBytes());
        ir.addInsert(3, "foo".getBytes(), "v1".getBytes());
        ir.addInsert(3, "bar".getBytes(), "v2".getBytes());
        db.insert(ir, null).get();
        
        // create a snapshot (in memory)
        database.getSnapshotManager().createPersistentSnapshot(
            "test",
            new DefaultSnapshotConfig("snap1", new int[] { 0, 3 }, new byte[][][] { { "test".getBytes() },
                { "ba".getBytes(), "blub".getBytes(), "f".getBytes() } }, new byte[][][] {
                { "testabc".getBytes() }, null }));
        DatabaseRO snap1 = database.getSnapshotManager().getSnapshotDB("test", "snap1");
        
        // overwrite original values
        ir = db.createInsertGroup();
        ir.addInsert(0, "testxyz".getBytes(), null);
        ir.addInsert(0, "test".getBytes(), "x".getBytes());
        ir.addInsert(0, "test2".getBytes(), "x".getBytes());
        ir.addInsert(0, "yagga".getBytes(), "x".getBytes());
        ir.addInsert(3, "foo".getBytes(), "x".getBytes());
        ir.addInsert(3, "bar".getBytes(), "x".getBytes());
        db.insert(ir, null).get();
        
        // check whether the snapshot contains exactly the specified key-value
        // pairs
        assertEquals("v1", new String(snap1.lookup(0, "testxyz".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(0, "test".getBytes(), null).get()));
        assertNull(snap1.lookup(0, "testabc".getBytes(), null).get());
        assertNull(snap1.lookup(0, "yagga".getBytes(), null).get());
        assertEquals("v1", new String(snap1.lookup(3, "foo".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(3, "bar".getBytes(), null).get()));
        assertNull(snap1.lookup(0, "test2".getBytes(), null).get());
        
        Iterator<Entry<byte[], byte[]>> it = snap1.prefixLookup(0, null, null).get();
        Entry<byte[], byte[]> next = it.next();
        assertEquals("test", new String(next.getKey()));
        assertEquals("v2", new String(next.getValue()));
        next = it.next();
        assertEquals("testxyz", new String(next.getKey()));
        assertEquals("v1", new String(next.getValue()));
        assertFalse(it.hasNext());
        
        // create a checkpoint
        database.getCheckpointer().checkpoint();
        
        // check whether the snapshot contains exactly the specified key-value
        // pairs
        assertEquals("v1", new String(snap1.lookup(0, "testxyz".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(0, "test".getBytes(), null).get()));
        assertNull(snap1.lookup(0, "testabc".getBytes(), null).get());
        assertNull(snap1.lookup(0, "yagga".getBytes(), null).get());
        assertEquals("v1", new String(snap1.lookup(3, "foo".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(3, "bar".getBytes(), null).get()));
        assertNull(snap1.lookup(0, "test2".getBytes(), null).get());
        
        it = snap1.prefixLookup(0, null, null).get();
        next = it.next();
        assertEquals("test", new String(next.getKey()));
        assertEquals("v2", new String(next.getValue()));
        next = it.next();
        assertEquals("testxyz", new String(next.getKey()));
        assertEquals("v1", new String(next.getValue()));
        assertFalse(it.hasNext());
        
    }
    
    public void testShutdownRestart() throws Exception {
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        Database db = database.getDatabaseManager().createDatabase("test", 4);
        
        // add some key-value pairs
        DatabaseInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "testxyz".getBytes(), "v1".getBytes());
        ir.addInsert(0, "test".getBytes(), "v2".getBytes());
        ir.addInsert(0, "yagga".getBytes(), "bla".getBytes());
        ir.addInsert(3, "foo".getBytes(), "v1".getBytes());
        ir.addInsert(3, "bar".getBytes(), "v2".getBytes());
        db.insert(ir, null).get();
        
        // create a snapshot (in memory)
        database.getSnapshotManager().createPersistentSnapshot("test",
            new DefaultSnapshotConfig("snap1", new int[] { 0, 3 }, null, null));
        DatabaseRO snap1 = database.getSnapshotManager().getSnapshotDB("test", "snap1");
        
        // overwrite original values
        ir = db.createInsertGroup();
        ir.addInsert(0, "testxyz".getBytes(), "x".getBytes());
        ir.addInsert(0, "test".getBytes(), "x".getBytes());
        ir.addInsert(3, "foo".getBytes(), "x".getBytes());
        ir.addInsert(3, "bar".getBytes(), "x".getBytes());
        db.insert(ir, null).get();
        
        // restart the database
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        db = database.getDatabaseManager().getDatabase("test");
        snap1 = database.getSnapshotManager().getSnapshotDB("test", "snap1");
        
        // check whether the snapshot exists and contains the correct value
        assertEquals("v1", new String(snap1.lookup(0, "testxyz".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(0, "test".getBytes(), null).get()));
        assertEquals("v1", new String(snap1.lookup(3, "foo".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(3, "bar".getBytes(), null).get()));
        
        // create a checkpoint and restart the database again
        database.getCheckpointer().checkpoint();
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        db = database.getDatabaseManager().getDatabase("test");
        snap1 = database.getSnapshotManager().getSnapshotDB("test", "snap1");
        
        // check whether the snapshot exists and contains the correct value
        assertEquals("v1", new String(snap1.lookup(0, "testxyz".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(0, "test".getBytes(), null).get()));
        assertEquals("v1", new String(snap1.lookup(3, "foo".getBytes(), null).get()));
        assertEquals("v2", new String(snap1.lookup(3, "bar".getBytes(), null).get()));
        
        ir = db.createInsertGroup();
        ir.addInsert(0, "te".getBytes(), "x".getBytes());
        ir.addInsert(0, "key".getBytes(), "x".getBytes());
        db.insert(ir, null).get();
        
        // create a partial user-defined snapshot
        database.getSnapshotManager().createPersistentSnapshot("test", new TestSnapshotConfig());
        
        // checkpoint and restart the database
        database.getCheckpointer().checkpoint();
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        
        DatabaseRO snap2 = database.getSnapshotManager().getSnapshotDB("test", "snap2");
        
        // check whether only the records covered by the prefixes are contained
        assertEquals("x", new String(snap2.lookup(0, "test".getBytes(), null).get()));
        assertNull(snap2.lookup(0, "testxyz".getBytes(), null).get());
        assertNull(snap2.lookup(0, "te".getBytes(), null).get());
        assertNull(snap2.lookup(0, "key".getBytes(), null).get());
        
    }
    
    public void testDelete() throws Exception {
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        Database db = database.getDatabaseManager().createDatabase("test", 4);
        
        // add some key-value pairs
        DatabaseInsertGroup ir = db.createInsertGroup();
        ir.addInsert(0, "testxyz".getBytes(), "v1".getBytes());
        ir.addInsert(0, "test".getBytes(), "v2".getBytes());
        ir.addInsert(0, "yagga".getBytes(), "bla".getBytes());
        ir.addInsert(1, "foo".getBytes(), "v1".getBytes());
        ir.addInsert(1, "bar".getBytes(), "v2".getBytes());
        db.insert(ir, null).get();
        
        // create a snapshot
        database.getSnapshotManager().createPersistentSnapshot("test",
            new DefaultSnapshotConfig("snap1", new int[] { 0, 3 }, null, null));
        
        // delete the snapshot
        database.getSnapshotManager().deletePersistentSnapshot("test", "snap1");
        try {
            database.getSnapshotManager().getSnapshotDB("test", "snap1");
            fail();
        } catch (Exception exc) {
            // ok
        }
        
        ir = db.createInsertGroup();
        ir.addInsert(0, "testxyz".getBytes(), "ggg".getBytes());
        ir.addInsert(0, "test".getBytes(), "ggg".getBytes());
        db.insert(ir, null).get();
        
        // restart the database w/o checkpointing
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        
        // checkpoint and restart the database
        database.getCheckpointer().checkpoint();
        database.shutdown();
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(baseDir, baseDir, 1, 0, 0,
            SyncMode.SYNC_WRITE, 0, 0, COMPRESSION, maxNumRecs, maxBlockFileSize, !MMAP, -1, DEBUG_LEVEL));
        
        // check if the formerly deleted snapshot does not exist anymore
        try {
            database.getSnapshotManager().getSnapshotDB("test", "snap1");
            fail();
        } catch (Exception exc) {
            // ok
        }
        
        // create a new snapshot
        database.getSnapshotManager().createPersistentSnapshot("test",
            new DefaultSnapshotConfig("snap1", new int[] { 0, 1 }, null, null));
        
        // checkpoint the database
        database.getCheckpointer().checkpoint();
        
        // delete the snapshot
        database.getSnapshotManager().deletePersistentSnapshot("test", "snap1");
        
        // check if the formerly deleted snapshot does not exist anymore
        try {
            database.getSnapshotManager().getSnapshotDB("test", "snap1");
            fail();
        } catch (Exception exc) {
            // ok
        }
        
        // create a new snapshot
        database.getSnapshotManager().createPersistentSnapshot("test",
            new DefaultSnapshotConfig("snap1", new int[] { 0, 1 }, null, null));
        
        // checkpoint the database
        database.getCheckpointer().checkpoint();
        
        // delete the database
        database.getDatabaseManager().deleteDatabase("test");
        
    }
    
    public static void main(String[] args) {
        TestRunner.run(SnapshotTest.class);
    }
    
    static class TestSnapshotConfig implements SnapshotConfig {
        private static final long serialVersionUID = 2439296996978183963L;
        
        @Override
        public boolean containsKey(int index, byte[] key) {
            if (index == 0
                && DefaultByteRangeComparator.getInstance().compare("testxyz".getBytes(), key) == 0)
                return false;
            return true;
        }
        
        @Override
        public int[] getIndices() {
            return new int[] { 0 };
        }
        
        @Override
        public String getName() {
            return "snap2";
        }
        
        @Override
        public byte[][] getPrefixes(int index) {
            if (index == 0)
                return new byte[][] { "test".getBytes() };
            else
                return null;
        }
        
    }
    
}