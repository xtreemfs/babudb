/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * 
 * @author bjko
 */
public class DiskLoggerTest extends TestCase {
    
    public static final String testdir = "/tmp/xtfs-dbtest/dbl/";
    
    private DiskLogger         l;
    
    public DiskLoggerTest() {
        Logging.start(Logging.LEVEL_ERROR);
    }
    
    @BeforeClass
    public static void setUpClass() throws Exception {
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        FSUtils.delTree(new File(testdir));
        l = new DiskLogger(testdir, new LSN(1, 1L), SyncMode.FSYNC, 0, 0);
        l.start();
        l.waitForStartup();
    }
    
    @After
    public void tearDown() throws Exception {
        l.shutdown();
        l.waitForShutdown();
    }
    
    @Test
    public void testSwitchLogFile() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        SyncListener sl = new SyncListener() {
            
            public void synced(LSN lsn) {
                synchronized (count) {
                    count.incrementAndGet();
                    count.notifyAll();
                }
            }
            
            public void failed(Exception ex) {
                synchronized (count) {
                    count.incrementAndGet();
                    count.notifyAll();
                }
            }
        };
        
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            l.append(new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT));
        }
        synchronized (count) {
            if (count.get() < 100)
                count.wait(1000);
        }
        
        try {
            l.lock();
            l.switchLogFile(false);
        } finally {
            l.unlock();
        }
        
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 100 + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            l.append(new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT));
        }
        
        synchronized (count) {
            if (count.get() < 200)
                count.wait(1000);
        }
    }
    
    @Test
    public void testReadLogfile() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        SyncListener sl = new SyncListener() {
            
            public void synced(LSN lsn) {
                synchronized (count) {
                    count.incrementAndGet();
                    count.notifyAll();
                    // System.out.println("wrote Entry: " +
                    // entry.getLogSequenceNo());
                }
            }
            
            public void failed(Exception ex) {
                synchronized (count) {
                    count.incrementAndGet();
                    count.notifyAll();
                }
            }
        };
        
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            l.append(new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT));
        }
        synchronized (count) {
            if (count.get() < 100)
                count.wait(1000);
        }
        
        System.out.println("finished writing");
        
        try {
            l.lock();
            l.switchLogFile(false);
        } finally {
            l.unlock();
        }
        
        DiskLogFile f = new DiskLogFile(testdir + "1.1.dbl");
        while (f.hasNext()) {
            LogEntry tmp = f.next();
            // byte[] data = tmp.getPayload().array();
            // String s = new String(data);
            // System.out.println("item: " + s);
            tmp.free();
        }
    }
    
    @Test
    public void testDefectiveEntries() throws Exception {
        
        File logFile = new File(testdir + "1.1.dbl");
        int[] offsets = new int[100];
        
        final AtomicInteger count = new AtomicInteger(0);
        int totalSize = 0;
        
        SyncListener sl = new SyncListener() {
            
            public void synced(LSN lsn) {
                synchronized (count) {
                    count.incrementAndGet();
                    count.notifyAll();
                }
            }
            
            public void failed(Exception ex) {
                fail("this should not happen");
                
                synchronized (count) {
                    count.incrementAndGet();
                    count.notifyAll();
                }
            }
        };
        
        for (int i = 0; i < 100; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            LogEntry e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
            int entrySize = LogEntry.headerLength + e.getPayload().remaining();
            
            if (i < 99)
                offsets[i + 1] = offsets[i] + entrySize;
            else
                totalSize = offsets[offsets.length - 1] + entrySize;
            l.append(e);
        }
        synchronized (count) {
            while (count.get() < 100)
                count.wait(1000);
        }
        
        System.out.println("finished writing");
        
        try {
            l.lock();
            l.switchLogFile(false);
        } finally {
            l.unlock();
        }
        
        assertEquals(totalSize, logFile.length());
        
        File tmpFile = new File(testdir + "log.dbl");
        copyFile(logFile, tmpFile);
        
        // write incorrect data in the first entr...
        RandomAccessFile raf = new RandomAccessFile(tmpFile.getAbsolutePath(), "rw");
        raf.seek(Integer.SIZE / 8);
        raf.writeInt(999999);
        raf.close();
        
        DiskLogFile f = new DiskLogFile(tmpFile.getAbsolutePath());
        assertFalse(f.hasNext());
        f.close();
        
        assertTrue(tmpFile.delete());
        copyFile(logFile, tmpFile);
        
        // write an incorrect size in the first entry...
        raf = new RandomAccessFile(tmpFile.getAbsoluteFile(), "rw");
        raf.writeInt(2);
        raf.close();
        
        f = new DiskLogFile(tmpFile.getAbsolutePath());
        assertFalse(f.hasNext());
        f.close();
        
        assertTrue(tmpFile.delete());
        copyFile(logFile, tmpFile);
        
        // write a corrupted entry in the middle of the log file...
        raf = new RandomAccessFile(tmpFile.getAbsolutePath(), "rw");
        raf.seek(offsets[50]);
        raf.writeInt(79);
        raf.close();
        
        f = new DiskLogFile(tmpFile.getAbsolutePath());
        for (int i = 0; i < 50; i++) {
            LogEntry next = f.next();
            assertNotNull(next);
            next.free();
        }
        assertFalse(f.hasNext());
        f.close();
        
        assertTrue(tmpFile.delete());
        copyFile(logFile, tmpFile);
        
        // write a negative-length entry in the middle of the log file...
        raf = new RandomAccessFile(tmpFile.getAbsolutePath(), "rw");
        raf.seek(offsets[50]);
        raf.writeInt(-122);
        raf.close();
        
        f = new DiskLogFile(tmpFile.getAbsolutePath());
        for (int i = 0; i < 50; i++) {
            LogEntry next = f.next();
            assertNotNull(next);
            next.free();
        }
        assertFalse(f.hasNext());
        f.close();
        
        assertTrue(tmpFile.delete());
        copyFile(logFile, tmpFile);
        
        // write a truncated entry at the end of the log file...
        raf = new RandomAccessFile(tmpFile.getAbsolutePath(), "rw");
        raf.getChannel().truncate(offsets[99] + 5);
        raf.close();
        
        f = new DiskLogFile(tmpFile.getAbsolutePath());
        for (int i = 0; i < 99; i++) {
            LogEntry next = f.next();
            assertNotNull(next);
            next.free();
        }
        assertFalse(f.hasNext());
        f.close();
        
        // replace the old log file with the corrected log file
        logFile.delete();
        copyFile(tmpFile, logFile);
        tmpFile.delete();
        
        // restart the disk logger and append new log entries
        l.shutdown();
        l = new DiskLogger(testdir, new LSN(1, 200L), SyncMode.FSYNC, 0, 0);
        l.start();
        
        for (int i = 99; i < 120; i++) {
            String pl = "Entry " + (i + 1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            l.append(new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT));
        }
        synchronized (count) {
            if (count.get() < 121)
                count.wait(1000);
        }
        
        File[] logFiles = new File(testdir).listFiles();
        DiskLogIterator it = new DiskLogIterator(logFiles, null);
        for (int i = 0; i < 120; i++) {
            LogEntry next = it.next();
            assertNotNull(next);
            next.free();
        }
        assertFalse(it.hasNext());
        
    }
    
    @Test
    public void testLogIterator() throws Exception {
        
        final int numLogFiles = 3;
        
        // create multiple consecutive log files, each containing 100 log
        // entries
        for (int k = 0; k < numLogFiles; k++) {
            
            final AtomicInteger count = new AtomicInteger(0);
            
            SyncListener sl = new SyncListener() {
                
                public void synced(LSN lsn) {
                    synchronized (count) {
                        count.incrementAndGet();
                        count.notifyAll();
                    }
                }
                
                public void failed(Exception ex) {
                    synchronized (count) {
                        System.err.println(ex);
                        count.incrementAndGet();
                        count.notifyAll();
                    }
                }
            };
            
            for (int i = 0; i < 100; i++) {
                String pl = "Entry " + (k * 100 + i + 1);
                ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
                l.append(new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT));
            }
            synchronized (count) {
                if (count.get() < 100)
                    count.wait();
            }
            
            try {
                l.lock();
                // LSN lsn =
                l.switchLogFile(false);
            } finally {
                l.unlock();
            }
            
        }
        
        File[] logFiles = new File(testdir).listFiles();
        assertEquals(numLogFiles + 1, logFiles.length);
        
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
        
        // create and test iterators that starts at different LSNs
        for (int k : new int[] { 1, 100, 101, 200, 201, 300, 77, 112, 189, 222 }) {
            
            LSN lsn = new LSN(1, k);
            
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
    
    @Test
    public void testSyncListener() throws Exception {
        
        final int numEntries = 1000;
        final int numRuns = 10;
        
        LSN lsn = new LSN(1, 0);
        for (int j = 0; j < numRuns; j++) {
            
            final AtomicInteger count = new AtomicInteger(0);
            int totalSize = 0;
            
            SyncListener sl = new SyncListener() {
                
                public void synced(LSN lsn) {
                    synchronized (count) {
                        count.incrementAndGet();
                        count.notifyAll();
                    }
                }
                
                public void failed(Exception ex) {
                    fail("this should not happen");
                    
                    synchronized (count) {
                        count.incrementAndGet();
                        count.notifyAll();
                    }
                }
            };
            
            for (int i = 0; i < numEntries; i++) {
                
                String pl = "Entry " + (i + 1);
                ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
                LogEntry e = new LogEntry(plb, sl, LogEntry.PAYLOAD_TYPE_INSERT);
                int entrySize = LogEntry.headerLength + e.getPayload().remaining();
                
                totalSize += entrySize;
                l.append(e);
            }
            
            synchronized (count) {
                while (count.get() < numEntries)
                    count.wait(5);
            }
            
            LSN newLSN = null;
            try {
                l.lock();
                newLSN = l.switchLogFile(false);
            } finally {
                l.unlock();
            }
            
            File logFile = new File(testdir + lsn.getViewId() + "." + (lsn.getSequenceNo() + 1) + ".dbl");
            lsn = newLSN;
            assertEquals(totalSize, logFile.length());
            
        }
    }
    
    private static void copyFile(File src, File dst) throws Exception {
        FileInputStream in = new FileInputStream(src);
        FileOutputStream out = new FileOutputStream(dst);
        while (in.available() > 0) {
            int b = in.read();
            out.write(b);
        }
        in.close();
        out.close();
    }
    
    public static void main(String[] args) {
        TestRunner.run(DiskLoggerTest.class);
    }
}