/*
 * Copyright (c) 2008 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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
import java.util.LinkedList;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Writes entries to the on disc operations log and syncs after blocks of MAX_ENTRIES_PER_BLOCK.
 * @author bjko
 */
public class DiskLogger extends LifeCycleThread {

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
    private FileChannel                 channel;

    /**
     * Stream used to obtain the FileChannel and to flush file.
     */
    private RandomAccessFile            fos;

    /**
     * Used to sync file.
     */
    private FileDescriptor              fdes;

    /**
     * The LogEntries to be written to disk.
     */
    private final LinkedList<LogEntry>  entries = new LinkedList<LogEntry>();

    /**
     * If set to true the thread will shutdown.
     */
    private boolean                     quit = true;
    private boolean                     graceful = true;

    /**
     * LogFile name
     */
    private volatile String             logfileDir;

    /**
     * log sequence number to assign to assign to next log entry
     */
    private final AtomicLong            nextLogSequenceNo;

    /**
     * view Id to assign to entries
     */
    private final AtomicInteger         currentViewId;

    /**
     * current log file name
     */
    private volatile String             currentLogFileName;

    /**
     * Lock for switching log files atomically
     */
    private ReentrantLock               sync;

    private final SyncMode              syncMode;

    private final Integer               pseudoSyncWait;
    
    private final CRC32                 csumAlgo = new CRC32();
        
    private final int                   maxQ;
    
    /**
     * Max number of LogEntries to write before sync.
     */
    public static final int             MAX_ENTRIES_PER_BLOCK = 250;

    /**
     * Creates a new instance of DiskLogger
     * @param logfile Name and path of file to use for append log.
     * @param dbs
     * @throws java.io.FileNotFoundException If that file cannot be created.
     * @throws java.io.IOException If that file cannot be created.
     */
    public DiskLogger(String logfileDir, int viewId, long nextLSN, SyncMode syncMode,
            int pseudoSyncWait, int maxQ) throws FileNotFoundException, IOException {

        super("DiskLogger");
        
        if (logfileDir == null) {
            throw new RuntimeException("expected a non-null log file directory name!");
        }
        if (logfileDir.endsWith("/")) {
            this.logfileDir = logfileDir;
        } else {
            this.logfileDir = logfileDir + "/";
        }

        this.pseudoSyncWait = pseudoSyncWait;

        if(pseudoSyncWait > 0 && syncMode == SyncMode.ASYNC) {
        	Logging.logMessage(Logging.LEVEL_WARN, this,"When pseudoSyncWait is enabled (> 0)" +
        			" make sure that SyncMode is not ASYNC");
        }
        
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
        
        this.maxQ = maxQ;
        sync = new ReentrantLock();

        Logging.logMessage(Logging.LEVEL_INFO, this,"BabuDB disk logger writes log file with " +
                syncMode);
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
     * Appends an entry to the write queue. Is maxQ is set an reached this method blocks until
     * queue space becomes available.
     * @param entry to write
     * @throws InterruptedException if the entry could not be 
     */
    public synchronized void append(LogEntry entry) throws InterruptedException, 
            IllegalStateException {
        
        assert (entry != null);
        
        // wait for queue space to become available
        if (!quit && maxQ > 0 && entries.size() >= maxQ) {
            wait();
        }
        
        if (!quit) {

            assert (maxQ == 0 || entries.size() < maxQ);
            
            entries.add(entry);
            notifyAll();
        } else {
            throw new InterruptedException("Appending the LogEntry to the DiskLogger's " +
            		"queue was interrupted, due DiskLogger shutdown.");
        }
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
            int view = currentViewId.getAndIncrement();
            long seq = nextLogSequenceNo.getAndSet(1L) - 1L;
            lastSyncedLSN = new LSN(view, seq);
        } else {
            lastSyncedLSN = new LSN(currentViewId.get(), nextLogSequenceNo.get() - 1L);
        }
        
        final String newFileName = createLogFileName();
        channel.close();
        fos.close();   

        currentLogFileName = newFileName;

        File lf = new File(currentLogFileName);
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
    
    /* (non-Javadoc)
     * @see java.lang.Thread#start()
     */
    @Override
    public synchronized void start() {
        
        assert (quit);
        
        quit = false;
        super.start();
    }

    /**
     * Main loop.
     */
    public void run() {
        List<LogEntry> tmpE = new ArrayList<LogEntry>(MAX_ENTRIES_PER_BLOCK);
                
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "operational");

        notifyStarted();
        
        while (!quit) {
            try {
                
                // wait for an entry
                synchronized (this) {
                    if (entries.isEmpty()) {
                        wait();
                    }
                    
                    if (quit) {
                        break;
                        
                    // get some entries from the queue
                    } else {
                        LogEntry tmp = null;
                        while (tmpE.size() < MAX_ENTRIES_PER_BLOCK - 1 && 
                              (tmp = entries.poll()) != null) {

                            tmpE.add(tmp);
                        }
                        notifyAll();
                    }
                }
                
                processLogEntries(tmpE);
                
            } catch (IOException ex) {
                
                Logging.logError(Logging.LEVEL_ERROR, this, ex);
                
       
                for (LogEntry le : tmpE) {
                    le.getListener().failed(le, ex);
                }
                tmpE.clear();
            } catch (InterruptedException ex) {
                if (!quit) {
                    try {
                        close();
                    } catch (IOException e) {
                        Logging.logError(Logging.LEVEL_ERROR, this, e);
                    }
                    notifyCrashed(ex);
                    return;
                }
            }
        }
        
        try {
            
            // process pending requests on shutdown if graceful flag has not been reset
            if (graceful) {
                processLogEntries(entries);
            }
            
            close();
            Logging.logMessage(Logging.LEVEL_INFO, this, "disk logger shut down successfully");
            notifyStopped();
        } catch (Exception e) {
            notifyCrashed(e);
        }
    }

    /**
     * stops this thread gracefully
     */
    public synchronized void shutdown() {
        shutdown(true);
    }
    
    /**
     * method to shutdown the logger without ensuring all pending log entries to be written to the 
     * on disk log. may cause inconsistency. use shutdown by default.
     * 
     * @param graceful - flag to determine, if shutdown should process gracefully, or not.
     */
    public synchronized void shutdown(boolean graceful) {
        this.graceful = graceful;
        quit = true;
        notifyAll();
        
        // stop pseudoSyncWait, if shutdown is ungraceful
        if (!graceful && pseudoSyncWait > 0) {
            synchronized (pseudoSyncWait) {
                pseudoSyncWait.notify();
            }
        }
    }
    
    /**
     * NEVER USE THIS EXCEPT FOR UNIT TESTS! Kills the logger.
     */
    @Deprecated
    public void destroy() {
        stop();
        try {
            try {
                fdes.sync();
            } finally {
                fos.close();
            }
        } catch (IOException e) {
            /* ignored */
        }
    }
    
    /**
     * <p>Function is used by the Replication.</p>
     * 
     * @return the LSN of the latest inserted {@link LogEntry}.
     */
    public LSN getLatestLSN(){
        return new LSN(currentViewId.get(), nextLogSequenceNo.get() - 1L);
    }
    
    private String createLogFileName() {
        return logfileDir + createLogFileName(currentViewId.get(), nextLogSequenceNo.get());
    }
    
    /**
     * Closes the log-file. And frees remaining entries.
     * 
     * @throws IOException
     */
    private void close() throws IOException {    
        
        try {
            fdes.sync();
        } finally {
            try {
                fos.close();
            } finally {
            
                assert (graceful || entries.size() == 0);
                
                // clear pending requests, if available
                for (LogEntry le : entries) {
                    le.getListener().failed(le, new BabuDBException(ErrorCode.INTERRUPTED, 
                        "DiskLogger was shut down, before the entry could be written to " +
                        "the log-file"));
                    le.free();
                }
            }
        }
    }
    
    /**
     * Writes a list of log entries to the disk log.
     * 
     * @param entries
     * @throws IOException 
     * @throws InterruptedException 
     */
    private final void processLogEntries(List<LogEntry> entries) throws IOException, 
            InterruptedException {

        lockLogger();
        try { 
            for (LogEntry le : entries) {
                assert (le != null) : "Entry must not be null";
                int viewID = currentViewId.get();
                long seqNo = nextLogSequenceNo.getAndIncrement();
    
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
                        
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                            "Writing entry LSN(%d:%d) with %d bytes payload [%s] to log. " +
                            "[serialized %d bytes]", viewID, seqNo, 
                            le.getPayload().remaining(), new String(le.getPayload().array()), 
                            buffer.remaining());
                    
                    // write the LogEntry to the local disk
                    channel.write(buffer.getBuffer());
                    
                } finally {
                    csumAlgo.reset();
                    if (buffer != null) BufferPool.free(buffer);
                }
            }
            
            if (this.syncMode == SyncMode.FSYNC) {
                channel.force(true);
            } else if (this.syncMode == SyncMode.FDATASYNC) {
                channel.force(false);
            }
            for (LogEntry le : entries) { 
                le.getListener().synced(le); 
            }  
            entries.clear();
    
            if (pseudoSyncWait > 0) {
                synchronized (pseudoSyncWait) {
                    pseudoSyncWait.wait(pseudoSyncWait);
                }
            }
        } finally {
            unlockLogger();
        }
    }
}
