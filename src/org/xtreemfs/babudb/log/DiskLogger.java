/*
 * Copyright (c) 2008-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.log;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Writes entries to the on disc operations log and syncs after blocks of MAX_ENTRIES_PER_BLOCK.
 * @author bjko
 */
public class DiskLogger extends Thread {

    /**
     * how to write log entries to disk
     */
    public static enum SyncMode {
        /**
         * asynchronously write log entries (data is lost when system crashes).
         */
        ASYNC,
        /**
         * executes an fsync on the logfile before acknowledging the operation.
         */
        FSYNC,

        FDATASYNC,
        /**
         * synchronously writes the log entry to disk before ack. Does not
         * update the metadata.
         */
        SYNC_WRITE,
        /**
         * synchronously writes the log entry to disk and updates
         * the metadat before ack.
         */
        SYNC_WRITE_METADATA
    };

    /**
     * NIO FileChannel used to write ByteBuffers directly to file.
     */
    private FileChannel channel;

    /**
     * Stream used to obtain the FileChannel and to flush file.
     */
    private RandomAccessFile fos;

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
    private volatile String logfileDir;

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
    private volatile String currentLogFileName;

    /**
     * Lock for switching log files atomically
     */
    private ReentrantLock                sync;

    private final SyncMode               syncMode;

    private final int 	                 pseudoSyncWait;
        
    /**
     * Max number of LogEntries to write before sync.
     */
    public static final int MAX_ENTRIES_PER_BLOCK = 250;

    /**
     * Creates a new instance of DiskLogger
     * @param logfile Name and path of file to use for append log.
     * @param dbs
     * @throws java.io.FileNotFoundException If that file cannot be created.
     * @throws java.io.IOException If that file cannot be created.
     */
    public DiskLogger(String logfileDir, int viewId, long nextLSN, SyncMode syncMode,
            int pseudoSyncWait, int maxQ) throws FileNotFoundException, IOException {

        super("DiskLogger thr.");
        
        if (logfileDir == null) {
            throw new RuntimeException("expected a non-null logfile directory name!");
        }
        if (logfileDir.endsWith("/")) {
            this.logfileDir = logfileDir;
        } else {
            this.logfileDir = logfileDir + "/";
        }

        this.pseudoSyncWait = pseudoSyncWait;

        if(pseudoSyncWait > 0 && syncMode == SyncMode.ASYNC) 
        	Logging.logMessage(Logging.LEVEL_WARN, this,"When pseudoSyncWait is enabled (> 0) make sure that SyncMode is not ASYNC");
        
        this.currentViewId = new AtomicInteger(viewId);

        assert (nextLSN > 0);
        this.nextLogSequenceNo = new AtomicLong(nextLSN);

        this.currentLogFileName = createLogFileName();

        File lf = new File(this.currentLogFileName);
        if (!lf.getParentFile().exists() && !lf.getParentFile().mkdirs()) {
            throw new IOException("could not create parent directory for database log file");
        }

        this.syncMode = syncMode;
        String openMode = "";
        switch (syncMode) {
            case ASYNC:
            case FSYNC:
            case FDATASYNC: {openMode = "rw"; break;}
            case SYNC_WRITE : {openMode = "rwd"; break;}
            case SYNC_WRITE_METADATA : {openMode = "rws"; break;}
        }
        fos = new RandomAccessFile(lf, openMode);
        fos.setLength(0);
        channel = fos.getChannel();
        fdes = fos.getFD();
        if (maxQ > 0)
            entries = new LinkedBlockingQueue<LogEntry>(maxQ);
        else
            entries = new LinkedBlockingQueue<LogEntry>();
        
        quit = false;
        this.down = new AtomicBoolean(false);

        sync = new ReentrantLock();

        //final String sMode = (pseudoSyncWait == 0) ? "synchronous" : "asynchronous";
        //Logging.logMessage(Logging.LEVEL_INFO, this,"BabuDB disk logger is in "+sMode+" mode");
        Logging.logMessage(Logging.LEVEL_INFO, this,"BabuDB disk logger writes log file with "+syncMode);
    }

    private String createLogFileName() {
        return this.logfileDir + createLogFileName(this.currentViewId.get(), this.nextLogSequenceNo.get());
    }

    public static String createLogFileName(int viewId, long sequenceNo) {
        return viewId + "." + sequenceNo + ".dbl";
    }
    
    public static LSN disassembleLogFileName(String name) {
        String[] parts = name.split(".");
        assert (parts.length == 3);
        
        return new LSN(Integer.parseInt(parts[0]),Long.parseLong(parts[1]));
    }

    public long getLogFileSize() {
        return new File(this.currentLogFileName).length();
    }

    /**
     * Appends an entry to the write queue.
     * @param entry to write
     */
    public void append(LogEntry entry) throws InterruptedException {
        assert (entry != null);
        
        entries.put(entry);
    }

    public void lockLogger() throws InterruptedException {
        sync.lockInterruptibly();
    }
    
    public boolean hasLock() {
        return sync.isHeldByCurrentThread();
    }
    
    public void unlockLogger() {
        sync.unlock();
    }

    public LSN switchLogFile(boolean incrementViewId) throws IOException {
        if (!hasLock()) {
            throw new IllegalStateException("the lock is held by another thread or the logger is not locked.");
        }
        
        // get last synchronized LSN and increment the viewId if needed
        LSN lastSyncedLSN = null;
        if (incrementViewId){
            int view = this.currentViewId.getAndIncrement();
            long seq = this.nextLogSequenceNo.getAndSet(1L) - 1L;
            lastSyncedLSN = new LSN(view, seq);
        } else {
            lastSyncedLSN = new LSN(this.currentViewId.get(), 
                                    this.nextLogSequenceNo.get() - 1L);
        }
        
        final String newFileName = createLogFileName();
        this.channel.close();
        this.fos.close();   

        this.currentLogFileName = newFileName;

        File lf = new File(this.currentLogFileName);
        String openMode = "";
        switch (syncMode) {
            case ASYNC:
            case FSYNC: 
            case FDATASYNC: {openMode = "rw"; break;}
            case SYNC_WRITE : {openMode = "rwd"; break;}
            case SYNC_WRITE_METADATA : {openMode = "rws"; break;}
        }
        fos = new RandomAccessFile(lf, openMode);
        fos.setLength(0);
        channel = fos.getChannel();
        fdes = fos.getFD();
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "switched log files... " +
        		"new name: " + currentLogFileName);
        
        return lastSyncedLSN;
    }

    /**
     * Main loop.
     */
    public void run() {

        ArrayList<LogEntry> tmpE = new ArrayList<LogEntry>(MAX_ENTRIES_PER_BLOCK);
                
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "operational");

        CRC32 csumAlgo = new CRC32();

        down.set(false);
        while (!quit) {
            try {
                // wait for an entry
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
                        int viewID = this.currentViewId.get();
                        long seqNo = this.nextLogSequenceNo.getAndIncrement();

                        assert(le.getLSN() == null || 
                                (le.getLSN().getSequenceNo() == seqNo && 
                                        le.getLSN().getViewId() == viewID)) : 
                            "LogEntry (" + le.getPayloadType() + ") had " +
                            "unexpected LSN: " + le.getLSN() + "\n" + viewID +
                            ":" + seqNo + " was expected instead.";
                        
                        le.assignId(viewID, seqNo);
                        
                        ReusableBuffer buffer = null;
                        try {
                            buffer = le.serialize(csumAlgo);
                                               
                            // write the LogEntry to the local disk
                            channel.write(buffer.getBuffer());
                        } finally {
                            csumAlgo.reset();
                            if (buffer != null) BufferPool.free(buffer);
                        }
                    }
                    
                    if (this.syncMode == SyncMode.FSYNC)
                        channel.force(true);
                    else if (this.syncMode == SyncMode.FDATASYNC)
                        channel.force(false);
                                     
                    for (LogEntry le : tmpE) { 
                        le.getListener().synced(le); 
                    }  
                    tmpE.clear();
                    
                    if (pseudoSyncWait > 0) {
                        DiskLogger.sleep(pseudoSyncWait);
                    }
                } finally {
                    sync.unlock();
                }
                
            } catch (IOException ex) {
                Logging.logError(Logging.LEVEL_ERROR, this, ex);
                
                for (LogEntry le : tmpE) {
                    le.getListener().failed(le, ex);
                }
                tmpE.clear();
            } catch (InterruptedException ex) {
                /* ignored */
            }
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "disk logger shut down successfully");
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
            fdes.sync();
            fos.close();
        } catch (IOException ex) {
        } finally {
            super.finalize();
        }
    }

    public int getQLength() {
        return this.entries.size();
    }
    
    /**
     * <p>Function is used by the Replication.</p>
     * 
     * @return the LSN of the latest inserted {@link LogEntry}.
     */
    public LSN getLatestLSN(){
        return new LSN(this.currentViewId.get(),this.nextLogSequenceNo.get()-1L);
    }
    
    /**
     * <p>Function is used by the Replication.</p>
     * 
     * @return the actual logFile.
     */
    public String getLatestLogFileName(){
        return currentLogFileName;
    }
}
