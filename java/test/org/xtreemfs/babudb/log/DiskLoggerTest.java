/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;

/**
 * 
 * @author bjko
 */
public class DiskLoggerTest extends TestCase {
    
    public static final String testdir = "/tmp/xtfs-dbtest/dbl/";
    
    private DiskLogger         l;
    
    public DiskLoggerTest() {
        Logging.start(Logging.LEVEL_DEBUG);
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws IOException {
        l = new DiskLogger(testdir, 1, 1, SyncMode.FSYNC, 0, 0, null);
        l.start();
    }
    
    @After
    public void tearDown() {
        l.shutdown();
    }
    
    @Test
    public void testSwitchLogFile() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        SyncListener sl = new SyncListener() {
            
            public void synced(LogEntry entry) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                }
            }
            
            public void failed(LogEntry entry, Exception ex) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                }
            }
        };
        
        LogEntry e = null;
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        synchronized (e) {
            if (count.get() < 100)
                e.wait(1000);
        }
        e.free();
        
        try {
            l.lockLogger();
            l.switchLogFile(false);
        } finally {
            l.unlockLogger();
        }
        
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 100 + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        
        synchronized (e) {
            if (count.get() < 200)
                e.wait(1000);
        }
        e.free();
    }
    
    @Test
    public void testReadLogfile() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        SyncListener sl = new SyncListener() {
            
            public void synced(LogEntry entry) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                    System.out.println("wrote Entry: " + entry.getLogSequenceNo());
                }
            }
            
            public void failed(LogEntry entry, Exception ex) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                }
            }
        };
        
        LogEntry e = null;
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        synchronized (e) {
            if (count.get() < 100)
                e.wait(1000);
        }
        e.free();
        
        System.out.println("finished writing");
        
        try {
            l.lockLogger();
            l.switchLogFile(false);
        } finally {
            l.unlockLogger();
        }
        
        DiskLogFile f = new DiskLogFile(testdir + "1.1.dbl");
        while (f.hasNext()) {
            LogEntry tmp = f.next();
            byte[] data = tmp.getPayload().array();
            String s = new String(data);
            System.out.println("item: " + s);
            tmp.free();
        }
    }
    
    @Test
    public void testDefctiveEntries() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        SyncListener sl = new SyncListener() {
            
            public void synced(LogEntry entry) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                }
            }
            
            public void failed(LogEntry entry, Exception ex) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                }
            }
        };
        
        LogEntry e = null;
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        synchronized (e) {
            if (count.get() < 100)
                e.wait(1000);
        }
        e.free();
        
        System.out.println("finished writing");
        
        try {
            l.lockLogger();
            l.switchLogFile(false);
        } finally {
            l.unlockLogger();
        }
        
        // write incorrect data...
        RandomAccessFile raf = new RandomAccessFile(testdir + "1.1.dbl", "rw");
        raf.seek(Integer.SIZE / 8);
        raf.writeInt(999999);
        raf.close();
        
        LogEntry tmp = null;
        try {
            DiskLogFile f = new DiskLogFile(testdir + "1.1.dbl");
            while (f.hasNext()) {
                tmp = f.next();
                byte[] data = tmp.getPayload().array();
                String s = new String(data);
                System.out.println("item: " + s);
                tmp.free();
                tmp = null;
            }
            Assert.fail("expected exception");
        } catch (LogEntryException ex) {
        } finally {
            if (tmp != null)
                tmp.free();
        }
        
        // write incorrect data...
        raf = new RandomAccessFile(testdir + "1.1.dbl", "rw");
        raf.writeInt(2);
        raf.close();
        
        tmp = null;
        try {
            DiskLogFile f = new DiskLogFile(testdir + "1.1.dbl");
            while (f.hasNext()) {
                tmp = f.next();
                byte[] data = tmp.getPayload().array();
                String s = new String(data);
                System.out.println("item: " + s);
                tmp.free();
                tmp = null;
            }
            Assert.fail("expected exception");
        } catch (LogEntryException ex) {
        } finally {
            if (tmp != null)
                tmp.free();
        }
        
    }
    
    public void testLogIterator() throws Exception {
        
        final int numLogFiles = 3;
        
        // create multiple log files
        for (int k = 0; k < numLogFiles; k++) {
            
            final AtomicInteger count = new AtomicInteger(0);
            
            SyncListener sl = new SyncListener() {
                
                public void synced(LogEntry entry) {
                    synchronized (entry) {
                        count.incrementAndGet();
                        entry.notifyAll();
                    }
                }
                
                public void failed(LogEntry entry, Exception ex) {
                    synchronized (entry) {
                        count.incrementAndGet();
                        entry.notifyAll();
                    }
                }
            };
            
            LogEntry e = null;
            for (int i = 0; i < 100; i++) {
                String pl = "Entry " + (k * 100 + i + 1);
                ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
                e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
                l.append(e);
            }
            synchronized (e) {
                if (count.get() < 100)
                    e.wait(1000);
            }
            e.free();
            
            try {
                l.lockLogger();
                LSN lsn = l.switchLogFile(false);
            } finally {
                l.unlockLogger();
            }
            
        }
        
        File[] logFiles = new File(testdir).listFiles();
        
        // create and test an iterator that starts at LSN 0
        DiskLogIterator it = new DiskLogIterator(logFiles, LSMDatabase.NO_DB_LSN);
        assertTrue(it.hasNext());
        for (int i = 1; i <= numLogFiles * 100; i++) {
            LogEntry next = it.next();
            String entry = new String(next.getPayload().array());
            assertEquals("Entry " + i, entry);
            next.free();
        }
        assertFalse(it.hasNext());
        it.destroy();
        
        // create and test iterators that starts at a random LSN
        for (int k = 0; k < 5; k++) {
            
            LSN lsn = new LSN(1, (int) (Math.random() * numLogFiles * 100));
            
            it = new DiskLogIterator(logFiles, lsn);
            assertTrue(it.hasNext());
            for (int i = (int) lsn.getSequenceNo(); i <= numLogFiles * 100; i++) {
                LogEntry next = it.next();
                String entry = new String(next.getPayload().array());
                assertEquals("Entry " + i, entry);
                next.free();
            }
            assertFalse(it.hasNext());
            it.destroy();
        }
        
    }
    
    public static void main(String[] args) {
        TestRunner.run(DiskLoggerTest.class);
    }
}