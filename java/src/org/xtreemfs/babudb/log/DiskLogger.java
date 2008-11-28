/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.common.buffer.BufferPool;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.logging.Logging;

/**
 * Writes entries to the on disc operations log and syncs after blocks of MAX_ENTRIES_PER_BLOCK.
 * @author bjko
 */
public class DiskLogger extends Thread {

    /**
     * NIO FileChannel used to write ByteBuffers directly to file.
     */
    private FileChannel channel;

    /**
     * Stream used to obtain the FileChannel and to flush file.
     */
    private FileOutputStream fos;

    /**
     * Used to sync file.
     */
    private FileDescriptor fdes;

    /**
     * The LogEntries to be written to disk.
     */
    private LinkedBlockingQueue<LogEntry> entries;

    /**
     * If set to true the thread will shutdown.
     */
    private transient boolean quit;

    /**
     * If set to true the DiskLogger is down
     */
    private AtomicBoolean down;

    /**
     * LogFile name
     */
    private String logfileDir;

    /**
     *  if set to true, no fsync is executed after disk writes. DANGEROUS.
     */
    private final boolean noFsync;

    /**
     * log sequence number to assign to assign to next log entry
     */
    private final AtomicLong nextLogSequenceNo;

    /**
     * view Id to assign to entries
     */
    private final AtomicInteger currentViewId;

    /**
     * current log file name
     */
    private String currentLogFileName;

    /**
     * Lock for switching log files atomically
     */
    private ReentrantLock sync;

    /**
     * Max number of LogEntries to write before sync.
     */
    public static final int MAX_ENTRIES_PER_BLOCK = 100;

    /**
     * Creates a new instance of DiskLogger
     * @param logfile Name and path of file to use for append log.
     * @throws java.io.FileNotFoundException If that file cannot be created.
     * @throws java.io.IOException If that file cannot be created.
     */
    public DiskLogger(String logfileDir, int viewId, long nextLSN, boolean noFsync) throws FileNotFoundException, IOException {

        super("DiskLogger thr.");

        if (logfileDir == null) {
            throw new RuntimeException("expected a non-null logfile directory name!");
        }
        if (logfileDir.endsWith("/")) {
            this.logfileDir = logfileDir;
        } else {
            this.logfileDir = logfileDir + "/";
        }

        this.currentViewId = new AtomicInteger(viewId);

        assert (nextLSN > 0);
        this.nextLogSequenceNo = new AtomicLong(nextLSN);

        this.currentLogFileName = createLogFileName();

        File lf = new File(this.currentLogFileName);
        if (!lf.getParentFile().exists() && !lf.getParentFile().mkdirs()) {
            throw new IOException("could not create parent directory for database log file");
        }
        fos = new FileOutputStream(lf, false);
        channel = fos.getChannel();
        fdes = fos.getFD();
        entries = new LinkedBlockingQueue();
        quit = false;
        this.down = new AtomicBoolean(false);
        this.noFsync = noFsync;

        sync = new ReentrantLock();
    }

    private String createLogFileName() {
        return this.logfileDir + createLogFileName(this.currentViewId.get(), this.nextLogSequenceNo.get());
    }

    public static String createLogFileName(int viewId, long sequenceNo) {
        return viewId + "." + sequenceNo + ".dbl";
    }

    public long getLogFileSize() {
        return new File(this.currentLogFileName).length();
    }

    /**
     * Appends an entry to the write queue.
     * @param entry entry to write
     */
    public void append(LogEntry entry) {
        assert (entry != null);
        entries.add(entry);
    }

    public void lockLogger() throws InterruptedException {
        sync.lockInterruptibly();
    }
    
    public void unlockLogger() {
        sync.unlock();
    }

    public LSN switchLogFile() throws IOException {
        if (!sync.isHeldByCurrentThread()) {
            throw new IllegalStateException("the lock is held by another thread or the logger is not locked.");
        }
        LSN lastSyncedLSN = new LSN(this.currentViewId.get(), this.nextLogSequenceNo.get() - 1);
        final String newFileName = createLogFileName();
        fos.close();
        channel.close();

        currentLogFileName = newFileName;

        File lf = new File(this.currentLogFileName);
        fos = new FileOutputStream(lf, false);
        channel = fos.getChannel();
        fdes = fos.getFD();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "switched log files... new name: " + currentLogFileName);
        return lastSyncedLSN;
    }

    /**
     * Main loop.
     */
    public void run() {

        ArrayList<LogEntry> tmpE = new ArrayList(MAX_ENTRIES_PER_BLOCK);
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "operational");

        CRC32 csumAlgo = new CRC32();

        down.set(false);
        while (!quit) {
            try {
                //wait for an entry
                tmpE.add(entries.take());
                sync.lockInterruptibly();
                try {
                    if (entries.size() > 0) {
                        while (tmpE.size() < MAX_ENTRIES_PER_BLOCK - 1) {
                            LogEntry tmp = entries.poll();
                            if (tmp == null) {
                                break;
                            }
                            tmpE.add(tmp);
                        }
                    }
                    for (LogEntry le : tmpE) {
                        assert (le != null) : "Entry must not be null";
                        le.assignId(this.currentViewId.get(), this.nextLogSequenceNo.getAndIncrement());
                        ReusableBuffer buf = le.serialize(csumAlgo);
                        csumAlgo.reset();
                        channel.write(buf.getBuffer());
                        BufferPool.free(buf);
                    }
                    fos.flush();
                    if (!noFsync) {
                        fdes.sync();
                    }
                    for (LogEntry le : tmpE) {
                        le.getListener().synced(le);
                    }
                    tmpE.clear();
                } finally {
                    sync.unlock();
                }
            } catch (SyncFailedException ex) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, ex);
                for (LogEntry le : tmpE) {
                    le.getListener().failed(le, ex);
                }
                tmpE.clear();
            } catch (IOException ex) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, ex);
                for (LogEntry le : tmpE) {
                    le.getListener().failed(le, ex);
                }
                tmpE.clear();
            } catch (InterruptedException ex) {
            }

        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "shutdown complete");
        synchronized (down) {
            down.set(true);
            down.notifyAll();
        }
    }

    /**
     * stops this thread
     */
    public void shutdown() {
        quit = true;
        synchronized (this) {
            this.interrupt();
        }
    }

    public boolean isDown() {
        return down.get();
    }

    public void waitForShutdown() throws InterruptedException {
        synchronized (down) {
            if (!down.get()) {
                down.wait();
            }
        }
    }

    /**
     * shut down files.
     */
    protected void finalize() throws Throwable {
        try {
            fos.flush();
            fdes.sync();
            fos.close();
        } catch (SyncFailedException ex) {
        } catch (IOException ex) {
        } finally {
            super.finalize();
        }
    }

    public int getQLength() {
        return this.entries.size();
    }
}
