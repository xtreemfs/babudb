/*
 * Copyright (c) 2009-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import static org.xtreemfs.babudb.BabuDBFactory.BABUDB_VERSION;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.Checkpointer;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.api.SnapshotManager;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.conversion.AutoConverter;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.VersionManagement;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;

/**
 * BabuDB main class.
 * 
 * <p>
 * <b>Please use the {@link BabuDBFactory} to generate instances of
 * {@link BabuDB}.</b>
 * </p>
 * 
 * @author bjko
 * @author flangner
 * @author stenjan
 * 
 */
public class BabuDBImpl implements BabuDBInternal, LifeCycleListener {
    
    private DiskLogger                   logger;
    
    private LSMDBWorker[]                worker;
    
    /**
     * the disk logger is used to write InsertRecordGroups persistently to disk
     */
    private PersistenceManager           permMan;
    
    /**
     * Checkpointer thread for automatic checkpointing
     */
    private final CheckpointerImpl       dbCheckptr;
    
    /**
     * the component that manages database snapshots
     */
    private final SnapshotManagerImpl    snapshotManager;
    
    /**
     * the component that manages databases
     */
    private final DatabaseManagerImpl    databaseManager;
    
    /**
     * All necessary parameters to run the BabuDB.
     */
    private final BabuDBConfig           configuration;
    
    /**
     * File used to store meta-informations about DBs.
     */
    private final DBConfig               dbConfigFile;
    
    /**
     * Flag that shows the replication if babuDB is running at the moment.
     */
    private final AtomicBoolean          stopped = new AtomicBoolean(true);
    
    /**
     * Threads used within the plugins. They will be stopped when BabuDB shuts
     * down.
     */
    private final List<LifeCycleThread>  plugins = new Vector<LifeCycleThread>();
    
    static {
        
        ReusableBuffer.enableAutoFree(true);
        // BufferPool.enableStackTraceRecording(true);
        
        // Check if the correct version of the foundation JAR is available. If
        // this is not the case, an error is thrown immediately on startup.
        // Whenever a new foundation JAR is imported, the required version
        // number should be adjusted, so as to make sure that BabuDB behaves as
        // expected.
        final int requiredFoundationVersion = 1;
        
        if (VersionManagement.getFoundationVersion() != requiredFoundationVersion) {
            throw new Error("Foundation.jar in classpath is required in version " + requiredFoundationVersion
                + "; present version is " + VersionManagement.getFoundationVersion());
        }
    }
    
    /**
     * Initializes the basic components of the BabuDB database system.
     * 
     * @param configuration
     * @throws BabuDBException
     */
    BabuDBImpl(BabuDBConfig configuration) throws BabuDBException {
        
        this.configuration = configuration;
        this.databaseManager = new DatabaseManagerImpl(this);
        this.dbConfigFile = new DBConfig(this);
        this.snapshotManager = new SnapshotManagerImpl(this);
        this.dbCheckptr = new CheckpointerImpl(this);
        this.permMan = new PersistenceManagerImpl();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#init(
     * org.xtreemfs.babudb.StaticInitialization)
     */
    @Override
    public void init(final StaticInitialization staticInit) throws BabuDBException {
        
        snapshotManager.init();
        
        // determine the LSN from which to start the log replay
        
        // to be able to recover from crashes during checkpoints, it is
        // necessary to start with the smallest LSN found on disk
        LSN dbLsn = null;
        for (Database db : databaseManager.getDatabaseList()) {
            if (dbLsn == null)
                dbLsn = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
            else {
                LSN onDiskLSN = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
                if (!(LSMDatabase.NO_DB_LSN.equals(dbLsn) || LSMDatabase.NO_DB_LSN.equals(onDiskLSN)))
                    dbLsn = dbLsn.compareTo(onDiskLSN) < 0 ? dbLsn : onDiskLSN;
            }
        }
        if (dbLsn == null) {
            // empty database
            dbLsn = new LSN(0, 0);
        } else {
            // need next LSN which is onDisk + 1
            dbLsn = new LSN(dbLsn.getViewId() == 0 ? 1 : dbLsn.getViewId(), dbLsn.getSequenceNo() + 1);
        }
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "starting log replay at LSN %s", dbLsn);
        LSN nextLSN = replayLogs(dbLsn);
        if (dbLsn.compareTo(nextLSN) > 0) {
            nextLSN = dbLsn;
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done, using LSN: " + nextLSN);
        
        // set up and start the disk logger
        try {
            this.logger = new DiskLogger(configuration.getDbLogDir(), nextLSN.getViewId(), nextLSN
                    .getSequenceNo(), configuration.getSyncMode(), configuration.getPseudoSyncWait(),
                configuration.getMaxQueueLength() * Math.max(1, configuration.getNumThreads()));
            this.logger.start();
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
        }
        this.permMan.setLogger(this.logger);
        
        if (configuration.getNumThreads() > 0) {
            worker = new LSMDBWorker[configuration.getNumThreads()];
            for (int i = 0; i < configuration.getNumThreads(); i++) {
                worker[i] = new LSMDBWorker(logger, i, (configuration.getPseudoSyncWait() > 0), configuration
                        .getMaxQueueLength());
                worker[i].start();
            }
        } else {
            // number of workers is 0 => requests will be responded directly.
            assert (configuration.getNumThreads() == 0);
            
            worker = null;
        }
        
        if (dbConfigFile.isConversionRequired())
            AutoConverter.completeConversion(this);
        
        // initialize and start the checkpointer; this has to be separated from
        // the instantiation because the instance has to be there when the log
        // is replayed
        this.dbCheckptr.init(logger, configuration.getCheckInterval(), configuration.getMaxLogfileSize());
        
        if (staticInit != null) {
            staticInit.initialize(databaseManager, snapshotManager);
        }
        
        this.stopped.set(false);
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is running " + "(version "
            + BABUDB_VERSION + ")");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#addPluginThread(
     * org.xtreemfs.foundation.LifeCycleThread)
     */
    @Override
    public void addPluginThread(LifeCycleThread plugin) {
        plugin.setLifeCycleListener(this);
        this.plugins.add(plugin);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#stop()
     */
    @Override
    public void stop() {
        synchronized (stopped) {
            if (stopped.get())
                return;
            
            if (worker != null)
                for (LSMDBWorker w : worker)
                    w.shutdown();
            
            logger.shutdown();
            try {
                this.dbCheckptr.suspendCheckpointing();
                logger.waitForShutdown();
                this.permMan.setLogger(null);
                if (worker != null)
                    for (LSMDBWorker w : worker)
                        w.waitForShutdown();
                
            } catch (InterruptedException ex) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "BabuDB could"
                    + " not be stopped, because '%s'.", ex.getMessage());
            }
            this.stopped.set(true);
            Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB has been " + "stopped.");
            
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#restart()
     */
    @Override
    public LSN restart() throws BabuDBException {
        synchronized (stopped) {
            
            if (!stopped.get()) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "BabuDB has to" + " be stopped before!");
            }
            
            databaseManager.reset();
            
            // determine the LSN from which to start the log replay
            
            // to be able to recover from crashes during checkpoints, it is
            // necessary to start with the smallest LSN found on disk
            LSN dbLsn = null;
            for (Database db : databaseManager.getDatabaseList()) {
                if (dbLsn == null)
                    dbLsn = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
                else {
                    LSN onDiskLSN = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
                    if (!(LSMDatabase.NO_DB_LSN.equals(dbLsn) || LSMDatabase.NO_DB_LSN.equals(onDiskLSN))) {
                        dbLsn = dbLsn.compareTo(onDiskLSN) < 0 ? dbLsn : onDiskLSN;
                    }
                }
            }
            if (dbLsn == null) {
                // empty database
                dbLsn = new LSN(0, 0);
            } else {
                // need next LSN which is onDisk + 1
                dbLsn = new LSN(dbLsn.getViewId(), dbLsn.getSequenceNo() + 1);
            }
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "starting log replay");
            LSN nextLSN = replayLogs(dbLsn);
            if (dbLsn.compareTo(nextLSN) > 0)
                nextLSN = dbLsn;
            Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done, " + "using LSN: " + nextLSN);
            
            try {
                logger = new DiskLogger(configuration.getDbLogDir(), nextLSN.getViewId(), nextLSN
                        .getSequenceNo(), configuration.getSyncMode(), configuration.getPseudoSyncWait(),
                    configuration.getMaxQueueLength() * configuration.getNumThreads());
                logger.start();
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR,
                    "Cannot start " + "database operations logger!", ex);
            }
            this.permMan.setLogger(logger);
            
            if (configuration.getNumThreads() > 0) {
                worker = new LSMDBWorker[configuration.getNumThreads()];
                for (int i = 0; i < configuration.getNumThreads(); i++) {
                    worker[i] = new LSMDBWorker(logger, i, (configuration.getPseudoSyncWait() > 0),
                        configuration.getMaxQueueLength());
                    worker[i].start();
                }
            } else {
                // number of workers is 0 => requests will be responded
                // directly.
                assert (configuration.getNumThreads() == 0);
                
                worker = null;
            }
            
            // restart the checkpointer
            this.dbCheckptr.init(logger, configuration.getCheckInterval(), configuration.getMaxLogfileSize());
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is " + "running (version "
                + BABUDB_VERSION + ")");
            
            this.stopped.set(false);
            return new LSN(nextLSN.getViewId(), nextLSN.getSequenceNo() - 1L);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown()
     */
    @Override
    public void shutdown() throws BabuDBException {
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "shutting down ...");
        
        if (worker != null) {
            for (LSMDBWorker w : worker) {
                w.shutdown();
            }
        }
        
        //stop the plugin threads TODO add shutdown method to LifeCycleThread
//        for (LifeCycleThread p : plugins) {
//            p.shutdown(); 
//            try {
//                p.waitForShutdown(); 
//            } catch (Exception e) { 
//                throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, 
//                        e.getMessage(), e.getCause()); 
//            } 
//        }
    
         

        try {
            
            // shut down the logger; this keeps insertions from being completed
            this.logger.shutdown();
            this.logger.waitForShutdown();
            
            // complete checkpoint before shutdown
            this.dbCheckptr.shutdown();
            this.dbCheckptr.waitForShutdown();
            
            this.databaseManager.shutdown();
            this.snapshotManager.shutdown();
            
            if (worker != null) {
                for (LSMDBWorker w : worker)
                    w.waitForShutdown();
                
                Logging.logMessage(Logging.LEVEL_DEBUG, this,
                    "%d worker " + "threads shut down successfully", worker.length);
            }
            
        } catch (InterruptedException ex) {
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB shutdown complete.");
    }
    
    /**
     * NEVER USE THIS EXCEPT FOR UNIT TESTS! Kills the database.
     */
    @SuppressWarnings("deprecation")
    public void __test_killDB_dangerous() {
        try {
            logger.stop();
            if (worker != null)
                for (LSMDBWorker w : worker)
                    w.stop();
            dbCheckptr.stop();
            
        } catch (IllegalMonitorStateException ex) {
            // we will probably get that when we kill a thread because we do
            // evil stuff here ;-)
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getCheckpointer()
     */
    @Override
    public Checkpointer getCheckpointer() {
        return dbCheckptr;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.BabuDBInternal#replacePersistenceManager(org.xtreemfs.babudb.api.PersistenceManager)
     */
    @Override
    public void replacePersistenceManager(PersistenceManager perMan) {
        this.permMan = perMan;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getPersistenceManager()
     */
    @Override
    public PersistenceManager getPersistenceManager() {
        return permMan;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getDatabaseManager()
     */
    @Override
    public DatabaseManager getDatabaseManager() {
        return databaseManager;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getConfig()
     */
    @Override
    public BabuDBConfig getConfig() {
        return configuration;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getSnapshotManager()
     */
    @Override
    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getDBConfigFile()
     */
    @Override
    public DBConfig getDBConfigFile() {
        return dbConfigFile;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getWorkerCount()
     */
    @Override
    public int getWorkerCount() {
        if (worker == null)
            return 0;
        return worker.length;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#getWorker(int)
     */
    @Override
    public LSMDBWorker getWorker(int dbId) {
        if (worker == null)
            return null;
        return worker[dbId % worker.length];
    }
    
    /**
     * Replays the database operations log.
     * 
     * @param from
     *            - LSN to replay the logs from.
     * @return the LSN to assign to the next operation
     * @throws BabuDBException
     */
    private LSN replayLogs(LSN from) throws BabuDBException {
        try {
            File f = new File(configuration.getDbLogDir());
            File[] logFiles = f.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".dbl");
                }
            });
            
            DiskLogIterator it = new DiskLogIterator(logFiles, from);
            LSN nextLSN = null;
            
            // apply log entries to databases ...
            while (it.hasNext()) {
                LogEntry le = null;
                try {
                    le = it.next();
                    
                    switch (le.getPayloadType()) {
                    
                    case LogEntry.PAYLOAD_TYPE_INSERT:
                        InsertRecordGroup ai = InsertRecordGroup.deserialize(le.getPayload());
                        databaseManager.insert(ai);
                        break;
                    
                    case LogEntry.PAYLOAD_TYPE_SNAP:
                        ObjectInputStream oin = null;
                        try {
                            oin = new ObjectInputStream(new ByteArrayInputStream(le.getPayload().array()));
                            
                            // deserialize the snapshot configuration
                            int dbId = oin.readInt();
                            SnapshotConfig snap = (SnapshotConfig) oin.readObject();
                            
                            Database db = databaseManager.getDatabase(dbId);
                            if (db == null)
                                break;
                            
                            snapshotManager.createPersistentSnapshot(db.getName(), snap, false);
                        } catch (Exception e) {
                            throw new BabuDBException(ErrorCode.IO_ERROR,
                                "Snapshot could not be recovered because: " + e.getMessage(), e);
                        } finally {
                            if (oin != null)
                                oin.close();
                        }
                        break;
                    
                    case LogEntry.PAYLOAD_TYPE_SNAP_DELETE:

                        byte[] payload = le.getPayload().array();
                        int offs = payload[0];
                        String dbName = new String(payload, 1, offs);
                        String snapName = new String(payload, offs + 1, payload.length - offs - 1);
                        
                        snapshotManager.deletePersistentSnapshot(dbName, snapName, false);
                        break;
                    
                    default: // create, copy and delete are skipped
                        break;
                    }
                    
                    // set LSN
                    nextLSN = new LSN(le.getViewId(), le.getLogSequenceNo() + 1L);
                } finally {
                    if (le != null)
                        le.free();
                }
                
            }
            
            it.destroy();
            
            if (nextLSN != null) {
                return nextLSN;
            } else {
                return new LSN(1, 1);
            }
            
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load "
                + "database operations log, file might be corrupted", ex);
        } catch (Exception ex) {
            if (ex.getCause() instanceof LogEntryException) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "corrupted/incomplete log entry in database "
                    + "operations log", ex.getCause());
            } else
                throw new BabuDBException(ErrorCode.IO_ERROR, "corrupted/incomplete log entry in database "
                    + "operations log", ex);
        }
    }
    
    /*
     * Life cycle listener used to handle crashes of plugins.
     */

    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() { /* ignored */
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() { /* ignored */
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(
     * java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable cause) {
        Logging.logError(Logging.LEVEL_ERROR, this, cause);
        try {
            shutdown();
        } catch (BabuDBException e) {
            Logging.logMessage(Logging.LEVEL_WARN, this, "BabuDB could not"
                + "have been terminated gracefully after plugin crash. " + "Because: %s", e.getMessage());
            e.printStackTrace();
        }
    }
}