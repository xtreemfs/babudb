/*
 * Copyright (c) 2008 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb.lsmdb;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.CheckpointerInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.util.OutputUtils;

/**
 * This thread regularly checks the size of the database operations log and
 * initiates a checkpoint of all databases if necessary.
 * 
 * @author bjko
 */
public class CheckpointerImpl extends Thread implements CheckpointerInternal {
    
    private final static class MaterializationRequest {
        
        MaterializationRequest(String dbName, int[] snapIDs, 
                SnapshotConfig snap) {
            
            this.dbName = dbName;
            this.snapIDs = snapIDs;
            this.snap = snap;
        }
        
        String         dbName;
        
        int[]          snapIDs;
        
        SnapshotConfig snap;
        
    }
    
    private volatile boolean                   quit;
    
    private final AtomicBoolean                down = 
        new AtomicBoolean(true);
    
    private final AtomicBoolean                suspended = 
        new AtomicBoolean(true);
    
    private DiskLogger                         logger;
    
    private long                               checkInterval;
    
    /**
     * Maximum file size of operations log in bytes.
     */
    private long                               maxLogLength;
    
    private final BabuDBInternal               dbs;
    
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
     * indicates when the current checkpoint is complete and is also a lock
     */
    private AtomicBoolean                      checkpointComplete = 
        new AtomicBoolean(true);
    
    /**
     * Flag to notify the disk-logger about a viewId incrementation.
     */
    private boolean                            incrementViewId    = false;
    private volatile LSN                       lastWrittenLSN;
    
    /**
     * Creates a new database checkpointer
     * 
     * @param master
     *            the database
     */
    public CheckpointerImpl(BabuDBInternal master) {
        super("ChkptrThr");
        this.dbs = master;
        this.requests = new LinkedList<MaterializationRequest>();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#init(
     *          org.xtreemfs.babudb.log.DiskLogger, int, long)
     */
    @Override
    public void init(DiskLogger logger, int checkInterval, long maxLogLength) {
        this.logger = logger;
        this.checkInterval = 1000l * checkInterval;
        this.maxLogLength = maxLogLength;

        synchronized (this) {
            
            synchronized (down) {
                if (down.getAndSet(false)) {
                    quit = false;  
                    start();
                }
            }
            
            synchronized (suspended) {
                this.suspended.set(false);
                this.suspended.notify();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#suspendCheckpointing()
     */
    @Override
    public synchronized void suspendCheckpointing() throws InterruptedException {
        
        synchronized (suspended) {
            if (!this.suspended.getAndSet(true)) {
                this.notify();
                this.suspended.wait();
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#checkpoint(boolean)
     */
    @Override
    public LSN checkpoint(boolean incViewId) throws BabuDBException {
        
        incrementViewId = incViewId;
        synchronized (checkpointComplete) {
            checkpointComplete.set(false);
        }
        
        // notify the checkpointing thread to immediately process all requests
        // in the processing queue
        synchronized (this) {
            forceCheckpoint = true;
            this.notify();
        }
        
        // wait for the checkpoint to complete
        try {
            waitForCheckpoint();
            return lastWrittenLSN;
        } catch (InterruptedException e) {
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "interrupted", e);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.lsmdb.Checkpointer#checkpoint()
     */
    @Override
    public void checkpoint() throws BabuDBException, InterruptedException {
        checkpoint(false);
    }
    
    /**
     * Materialize all snapshots in the queue before taking a checkpoint.
     * 
     * @throws BabuDBException if materialization failed.
     */
    private void materializeSnapshots() throws BabuDBException {
        
        for (;;) {
            MaterializationRequest rq = null;
            
            synchronized (requests) {
                if (requests.size() > 0)
                    rq = requests.remove(0);
            }
            
            if (rq == null)
                break;
            
            if(Logging.isDebug())
	            Logging.logMessage(Logging.LEVEL_DEBUG, this,
	                "snapshot materialization request found for database '" + 
	                rq.dbName + "', snapshot: '" + rq.snap.getName() + "'");
            
            SnapshotManagerImpl snapMan = 
                (SnapshotManagerImpl) dbs.getSnapshotManager();
            
            // write the snapshot
            dbs.getDatabaseManager().getDatabase(rq.dbName)
                    .proceedWriteSnapshot(rq.snapIDs, 
                            snapMan.getSnapshotDir(rq.dbName,
                            rq.snap.getName()), rq.snap);
            
            // notify the snapshot manager about the completion
            // of the snapshot
            snapMan.snapshotComplete(rq.dbName, rq.snap);
            
            Logging.logMessage(Logging.LEVEL_DEBUG, this,
                        "snapshot materialization complete");
        }
    }
    
    /**
     * Internal method for creating a new database checkpoint. This involves the
     * following steps:
     * <ol>
     * <li>snapshot all indices of all databases
     * <li>create new log file for subsequent insertions
     * <li>write index snapshots to new on-disk index files
     * <li>link new on-disk files to index structures
     * <li>delete any obsolete on-disk files
     * <li>delete any obsolete log files
     * </ol>
     * The first two steps need to be sync'ed with new insertions but should be
     * very fast. The following steps are performed in a fully asynchronous
     * manner.
     * 
     * @throws BabuDBException
     * @throws InterruptedException
     */
    private void createCheckpoint() throws BabuDBException, InterruptedException {
        Logging.logMessage(Logging.LEVEL_INFO, this, "initiating database checkpoint...");
        
        Collection<DatabaseInternal> databases = dbs.getDatabaseManager().getDatabaseList();
        
        try {
            int[][] snapIds = new int[databases.size()][];
            int i = 0;
            
            try {
                // critical block...
                logger.lockLogger();
                for (Database db : databases) {
                    snapIds[i++] = ((DatabaseImpl) db).proceedCreateSnapshot();
                }
                lastWrittenLSN = logger.switchLogFile(incrementViewId);
                incrementViewId = false;
            } finally {
                logger.unlockLogger();
            }
            
            i = 0;
            for (Database db : databases) {
                ((DatabaseImpl) db).proceedWriteSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN
                        .getSequenceNo(), snapIds[i++]);
                ((DatabaseImpl) db).proceedCleanupSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN
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
                        if (!f.delete())
                            Logging.logMessage(Logging.LEVEL_WARN, this, "could not delete log file: %s",
                                f.getAbsolutePath());
                    }
                }
            }
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot create checkpoint", ex);
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "checkpoint complete");
    }
    
    public void addSnapshotMaterializationRequest(String dbName, int[] snapIds, SnapshotConfig snap) {
        synchronized (requests) {
            requests.add(new MaterializationRequest(dbName, snapIds, snap));
        }
    }
    
    public void removeSnapshotMaterializationRequest(String dbName, 
            String snapshotName) {
        
        synchronized (requests) {
            Iterator<MaterializationRequest> it = requests.iterator();
            while (it.hasNext()) {
                MaterializationRequest rq = it.next();
                if (snapshotName.equals(rq.snap.getName())) {
                    requests.remove(rq);
                    break;
                }
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#shutdown()
     */
    @Override
    public synchronized void shutdown() {
        quit = true;
        this.interrupt();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#waitForShutdown()
     */
    @Override
    public void waitForShutdown() throws InterruptedException {
        synchronized (down) {
            if (!down.get())
                down.wait();
        }
    }
    
    public void run() {
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "operational");
        
        boolean manualCheckpoint = false;
        while (!quit) {
            synchronized (this) {
                
                // this block allows to suspend the Checkpointer from taking
                // checkpoints until it has been re-init()
                synchronized (suspended) {
                    if (suspended.get()) {
                        suspended.notify();
                        try {
                            suspended.wait();
                        } catch (InterruptedException ex) {
                            if (quit) break;
                        }
                    }
                }
                
                if (!forceCheckpoint) {
                    try {
                        this.wait(checkInterval);
                    } catch (InterruptedException ex) {
                        if (quit) break;
                    }
                }
                manualCheckpoint = forceCheckpoint;
                forceCheckpoint = false;
            }
            
            try {
                
                final long lfsize = logger.getLogFileSize();
                if (manualCheckpoint || lfsize > maxLogLength) {
                    
                    if (!manualCheckpoint)
                        Logging.logMessage(Logging.LEVEL_INFO, this,
                            "database operation log has exceeded threshold " +
                            "size of " + maxLogLength + " ("  + lfsize + ")");
                    else
                        Logging.logMessage(Logging.LEVEL_INFO, this, 
                                "triggered manual checkpoint");
                    
                    synchronized (((DatabaseManagerImpl) dbs.getDatabaseManager()).getDBModificationLock()) {
                        synchronized (this) {
                            materializeSnapshots();

                            createCheckpoint();
                        }                        
                    }                    
                }
            } catch (InterruptedException ex) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "CHECKPOINT WAS ABORTED!");
            } catch (Throwable ex) {
                if (ex instanceof BabuDBException && ((BabuDBException) ex).getCause() instanceof ClosedByInterruptException) {
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, "CHECKPOINT WAS ABORTED!");
                } else {
                    Logging.logMessage(Logging.LEVEL_ERROR, this, "DATABASE CHECKPOINT CREATION FAILURE!");
                    Logging.logMessage(Logging.LEVEL_ERROR, this, OutputUtils.stackTraceToString(ex));
                }
            } finally {
                synchronized (checkpointComplete) {
                    checkpointComplete.set(true);
                    checkpointComplete.notify();
                }
            }
        }
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "checkpointer shut down "
        		+ "successfully");
        synchronized (down) {
            down.set(true);
            down.notifyAll();
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.Checkpointer#waitForCheckpoint()
     */
    @Override
    public void waitForCheckpoint() throws InterruptedException {
        synchronized (checkpointComplete) {
            while (!checkpointComplete.get())
                checkpointComplete.wait();
        }
    }
}
