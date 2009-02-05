/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb.lsmdb;

import org.xtreemfs.babudb.BabuDBException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.common.logging.Logging;

/**
 * This thread regularly checks the size of the database operations log
 * and initiates a checkpoint of all databases if necessary.
 * @author bjko
 */
public class Checkpointer extends Thread {
    
    private transient boolean quit;
    
    private final AtomicBoolean down;
    
    private final DiskLogger    logger;
    
    private final long           checkInterval;
    
    /**
     * Maximum file size of operations log in bytes.
     */
    private final long          maxLogLength;
    
    private final BabuDB        master;
    
    /**
     * From the replication designated checkpoint.
     */
    private LSN                 recommended;
    
    /**
     * Creates a new database checkpointer
     * @param master the database
     * @param logger the disklogger
     * @param checkInterval interval in seconds between two checks
     * @param maxLogLength maximum log file length
     */
    public Checkpointer(BabuDB master, DiskLogger logger, int checkInterval, long maxLogLength) {
        super("ChkptrThr");
        down = new AtomicBoolean(false);
        this.logger = logger;
        this.checkInterval = 1000l*checkInterval;
        this.maxLogLength = maxLogLength;
        this.master = master;
    }
    
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
            if (!down.get())
                down.wait();
        }
    }
    
    public void run() {
        quit = false;
        down.set(false);
        Logging.logMessage(Logging.LEVEL_DEBUG,this,"operational");
        while (!quit) {
            synchronized (this) {
                try {
                    this.wait(checkInterval);
                } catch (InterruptedException ex) {
                    if (quit)
                        break;
                }
            }
            try {
                final long lfsize = logger.getLogFileSize();
                if (lfsize > this.maxLogLength) {
                    Logging.logMessage(Logging.LEVEL_INFO, this,"database operation log is too large ("+
                            lfsize+") initiating database checkpoint...");
                    master.checkpoint();
                    Logging.logMessage(Logging.LEVEL_INFO, this,"checkpoint complete.");

                }
            } catch (InterruptedException ex) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this,"CHECKPOINT WAS ABORTED!");
            } catch (BabuDBException ex) {
                Logging.logMessage(Logging.LEVEL_ERROR, this,"AUTOMATIC DATABASE CHECKPOINT FAILED!");
                Logging.logMessage(Logging.LEVEL_ERROR, this, ex);
            }
        }
        Logging.logMessage(Logging.LEVEL_DEBUG,this,"shutdown complete");
        synchronized (down) {
            down.set(true);
            down.notifyAll();
        }
    }

    /**
     * <p>For replication purpose.</p>
     * <p>A master can mark the LSN as recommended for the next checkpoint, if all slaves
     * have acknowledged replicas until LSN.</p>
     * 
     * @param lsn
     */
    public synchronized void designateRecommendedCheckpoint(LSN lsn){
            this.recommended = lsn;       
    }
}
