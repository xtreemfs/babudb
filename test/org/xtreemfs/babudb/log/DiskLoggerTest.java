/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.junit.Assert;
import org.xtreemfs.babudb.BabuDBTest;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

/**
 *
 * @author bjko
 */
public class DiskLoggerTest extends TestCase {

    public static final String testdir = "/tmp/xtfs-dbtest/dbl/";
    
    private DiskLogger l;
    
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
        l = new DiskLogger(testdir, 1, 1, SyncMode.FSYNC,0,0);
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
            String pl = "Entry "+(i+1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb,sl,LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        synchronized (e) {
            if (count.get() < 100)
                e.wait(1000);    
        }
                
        try {
            l.lockLogger();
            l.switchLogFile(false);
        } finally {
            l.unlockLogger();
        }
        
        for (int i = 0; i < 100; i++) {
            String pl = "Entry "+(i+100+1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb,sl,LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        
        synchronized (e) {
            if (count.get() < 200)
                e.wait(1000);    
        }
     }
    
    @Test
    public void testReadLogfile() throws Exception {
        
        final AtomicInteger count = new AtomicInteger(0);
        
        SyncListener sl = new SyncListener() {

            public void synced(LogEntry entry) {
                synchronized (entry) {
                    count.incrementAndGet();
                    entry.notifyAll();
                    System.out.println("wrote Entry: "+entry.getLogSequenceNo());
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
            String pl = "Entry "+(i+1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb,sl,LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        synchronized (e) {
            if (count.get() < 100)
                e.wait(1000);    
        }
        
        System.out.println("finished writing");
                
        try {
            l.lockLogger();
            l.switchLogFile(false);
        } finally {
            l.unlockLogger();
        }
        
        DiskLogFile f = new DiskLogFile(testdir+"1.1.dbl");
        while (f.hasNext()) {
            LogEntry tmp = f.next();
            byte[] data = tmp.getPayload().array();
            String s = new String(data);
            System.out.println("item: "+s);
            tmp.free();
        }
    }
    
    @Test
    public void testDfectiveEntries() throws Exception {
        
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
            String pl = "Entry "+(i+1);
            ReusableBuffer plb = ReusableBuffer.wrap(pl.getBytes());
            e = new LogEntry(plb,sl,LogEntry.PAYLOAD_TYPE_INSERT);
            l.append(e);
        }
        synchronized (e) {
            if (count.get() < 100)
                e.wait(1000);    
        }
        
        System.out.println("finished writing");
        
        try {
            l.lockLogger();
            l.switchLogFile(false);
        } finally {
            l.unlockLogger();
        }
        
        
        //write incorrect data...
        RandomAccessFile raf = new RandomAccessFile(testdir+"1.1.dbl","rw");
        raf.seek(Integer.SIZE/8);
        raf.writeInt(999999);
        raf.close();

        try {
            DiskLogFile f = new DiskLogFile(testdir+"1.1.dbl");
            while (f.hasNext()) {
                LogEntry tmp = f.next();
                byte[] data = tmp.getPayload().array();
                String s = new String(data);
                System.out.println("item: "+s);
                tmp.free();
            }
            Assert.fail("expected exception");
        } catch (LogEntryException ex) {
        }
        
        //write incorrect data...
        raf = new RandomAccessFile(testdir+"1.1.dbl","rw");
        raf.writeInt(2);
        raf.close();

        try {
            DiskLogFile f = new DiskLogFile(testdir+"1.1.dbl");
            while (f.hasNext()) {
                LogEntry tmp = f.next();
                byte[] data = tmp.getPayload().array();
                String s = new String(data);
                System.out.println("item: "+s);
                tmp.free();
            }
            Assert.fail("expected exception");
        } catch (LogEntryException ex) {
        }
                
    }
    
    public static void main(String[] args) {
        TestRunner.run(DiskLoggerTest.class);
    }
}