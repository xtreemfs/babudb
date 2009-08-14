/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import static org.xtreemfs.include.common.config.SlaveConfig.slaveProtection;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManager;
import org.xtreemfs.include.common.logging.Logging;

/**
 * This thread regularly checks the size of the database operations log and
 * initiates a checkpoint of all databases if necessary.
 * 
 * @author bjko
 */
public class Checkpointer extends Thread {
    
    static class MaterializationRequest {
        
        public MaterializationRequest(String dbName, int[] snapIDs, SnapshotConfig snap, SnapshotManager snMan) {
            this.dbName = dbName;
            this.snapIDs = snapIDs;
            this.snap = snap;
            this.snMan = snMan;
        }
        
        public String          dbName;
        
        public int[]           snapIDs;
        
        public SnapshotConfig  snap;
        
        public SnapshotManager snMan;
        
    }
    
    private transient boolean                  quit;
    
    private final AtomicBoolean                down;
    
    private final DiskLogger                   logger;
    
    private final long                         checkInterval;
    
    /**
     * Maximum file size of operations log in bytes.
     */
    private final long                         maxLogLength;
    
    private final BabuDB                       dbs;
        
    /**
     * a queue containing all snapshot materialization requests that should be
     * exectued before the next checkpoint is made
     */
    private final List<MaterializationRequest> requests;
    
    /**
     * indicates whether the next checkpoint has been triggered manually or
     * automatically
     */
    private boolean                            forceCheckpoint;
    
    /**
     * indicates when the current checkpoint is complete
     */
    private boolean                            checkpointComplete;
    
    /**
     * object used to ensure that only one checkpoint is created synchronously
     */
    private final Object                       checkpointLock;
    
    /**
     * object used to block the user-triggered checkpointing operation until
     * checkpoint creation has completed
     */
    private final Object                       checkpointCompletionLock;
    
    /**
     * Flag to notify the disk-logger about a viewId incrementation.
     */
    private boolean                            incrementViewId;
    
    /**
     * Creates a new database checkpointer
     * 
     * @param master
     *            the database
     * @param logger
     *            the disklogger
     * @param checkInterval
     *            interval in seconds between two checks
     * @param maxLogLength
     *            maximum log file length
     */
    public Checkpointer(BabuDB master, DiskLogger logger, int checkInterval, long maxLogLength) {
        super("ChkptrThr");
        down = new AtomicBoolean(false);
        this.logger = logger;
        this.checkInterval = 1000l * checkInterval;
        this.maxLogLength = maxLogLength;
        this.dbs = master;
        this.requests = new LinkedList<MaterializationRequest>();
        this.checkpointLock = new Object();
        this.checkpointCompletionLock = new Object();
    }
    
    /**
     * Triggers the creation of a new checkpoint. This causes the checkpointer
     * to run its checkpointing mechanism, which causes all outstanding snapshot
     * materialization requests to be processed. After this has been done, the
     * checkpoint will be created.
     * 
     * The method blocks until the checkpoints (and all outstanding snapshots)
     * have been persistently written to disk.
     * 
     * @param incViewId - set true, if the viewId should be incremented (replication issue).
     * 
     */
    public void checkpoint(boolean incViewId) throws BabuDBException {
        incrementViewId = incViewId;
        checkpointComplete = false;
        
        // notify the checkpointing thread to immediately process all requests
        // in the processing queue
        synchronized (this) {
            forceCheckpoint = true;
            this.notify();
        }
        
        // wait for the checkpoint to complete
        synchronized (checkpointCompletionLock) {
            if (!checkpointComplete)
                try {
                    checkpointCompletionLock.wait();
                } catch (InterruptedException e) {
                    throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "interrupted", e);
                }
        }
    }
    
    /**
     * Creates a checkpoint of all databases. The in-memory data is merged with
     * the on-disk data and is written to a new snapshot file. Database logs are
     * truncated. This operation is thread-safe.
     * 
     * @throws BabuDBException
     *             if the checkpoint was not successful
     * @throws InterruptedException
     */
    public void checkpoint() throws BabuDBException, InterruptedException {
        if (dbs.replication_isSlave()) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, slaveProtection);
        }
        
        checkpoint(false);
    }
    
    private void createCheckpoint() throws BabuDBException, InterruptedException {
        
        Collection<Database> databases = dbs.getDatabaseManager().getDatabases();
        
        synchronized (checkpointLock) {
            try {
                int[][] snapIds = new int[databases.size()][];
                int i = 0;
                
                LSN lastWrittenLSN = null;
                try {
                    // critical block...
                    logger.lockLogger();
                    for (Database db : databases) {
                        snapIds[i++] = ((DatabaseImpl) db).createSnapshot();
                    }
                    lastWrittenLSN = logger.switchLogFile(incrementViewId);
                } finally {
                    logger.unlockLogger();
                }
                
                i = 0;
                for (Database db : databases) {
                    ((DatabaseImpl) db).writeSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN
                            .getSequenceNo(), snapIds[i++]);
                    ((DatabaseImpl) db).cleanupSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN
                            .getSequenceNo());
                }
                
                // delete all logfile with LSN <= lastWrittenLSN
                File f = new File(dbs.getConfig().getDbLogDir());
                String[] logs = f.list(new FilenameFilter() {
                    
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".dbl");
                    }
                });
                if (logs != null) {
                    Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
                    for (String log : logs) {
                        Matcher m = p.matcher(log);
                        m.matches();
                        String tmp = m.group(1);
                        int viewId = Integer.valueOf(tmp);
                        tmp = m.group(2);
                        int seqNo = Integer.valueOf(tmp);
                        LSN logLSN = new LSN(viewId, seqNo);
                        if (logLSN.compareTo(lastWrittenLSN) <= 0) {
                            Logging.logMessage(Logging.LEVEL_DEBUG, this, "deleting old db log file: " + log);
                            f = new File(dbs.getConfig().getDbLogDir() + log);
                            f.delete();
                        }
                    }
                }
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot create checkpoint", ex);
            }
        }
    }
    
    public void addSnapshotMaterializationRequest(String dbName, int[] snapIds, SnapshotConfig snap,
        SnapshotManager snMan) {
        synchronized (requests) {
            requests.add(new MaterializationRequest(dbName, snapIds, snap, snMan));
        }
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
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "operational");
        
        boolean manualCheckpoint = false;
        while (!quit) {
            synchronized (this) {
                try {
                    this.wait(checkInterval);
                } catch (InterruptedException ex) {
                    if (quit)
                        break;
                }
                manualCheckpoint = forceCheckpoint;
                forceCheckpoint = false;
            }
            try {
                
                final long lfsize = logger.getLogFileSize();
                if (manualCheckpoint || lfsize > maxLogLength) {
                    Logging.logMessage(Logging.LEVEL_INFO, this,
                        "database operation log has exceeded threshold size of " + maxLogLength + " ("
                            + lfsize + ")");
                    
                    // materialize all snapshots in the queue before creating
                    // the checkpoint
                    for (;;) {
                        MaterializationRequest rq = null;
                        
                        synchronized (requests) {
                            if (requests.size() > 0)
                                rq = requests.remove(0);
                        }
                        
                        if (rq == null)
                            break;
                        
                        Logging.logMessage(Logging.LEVEL_DEBUG, this,
                            "snapshot materialization request found for database '" + rq.dbName
                                + "', snapshot: '" + rq.snap.getName() + "'");
                        
                        // write the snapshot
                        ((DatabaseImpl) dbs.getDatabaseManager().getDatabase(rq.dbName)).writeSnapshot(
                            rq.snapIDs, dbs.getSnapshotManager().getSnapshotDir(rq.dbName,
                                rq.snap.getName()), rq.snap);
                        
                        // notify the snapshot manager about the completion of
                        // the snapshot
                        rq.snMan.snapshotComplete(rq.dbName, rq.snap);
                        
                        Logging.logMessage(Logging.LEVEL_DEBUG, this, "snapshot materialization complete");
                    }
                    
                    // create the checkpoint
                    Logging.logMessage(Logging.LEVEL_INFO, this, "initiating database checkpoint...");
                    createCheckpoint();
                    Logging.logMessage(Logging.LEVEL_INFO, this, "checkpoint complete");
                                        
                }
                
            } catch (InterruptedException ex) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "CHECKPOINT WAS ABORTED!");
            } catch (Throwable ex) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "DATABASE CHECKPOINT CREATION FAILURE!");
                Logging.logMessage(Logging.LEVEL_ERROR, this, ex.getMessage());
            } finally {
                synchronized (checkpointCompletionLock) {
                    checkpointComplete = true;
                    checkpointCompletionLock.notify();
                }
            }
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "shutdown complete");
        synchronized (down) {
            down.set(true);
            down.notifyAll();
        }
    }
}
