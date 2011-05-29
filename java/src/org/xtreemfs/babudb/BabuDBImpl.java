/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import static org.xtreemfs.babudb.BabuDBFactory.BABUDB_VERSION;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_COPY;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_CREATE;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_DELETE;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_SNAP;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_SNAP_DELETE;
import static org.xtreemfs.babudb.log.LogEntry.PAYLOAD_TYPE_TRANSACTION;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.dev.CheckpointerInternal;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.dev.DatabaseManagerInternal;
import org.xtreemfs.babudb.api.dev.ResponseManagerInternal;
import org.xtreemfs.babudb.api.dev.SnapshotManagerInternal;
import org.xtreemfs.babudb.api.dev.transaction.InMemoryProcessing;
import org.xtreemfs.babudb.api.dev.transaction.OperationInternal;
import org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.conversion.AutoConverter;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.foundation.LifeCycleThread;
import org.xtreemfs.foundation.VersionManagement;
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
public class BabuDBImpl implements BabuDBInternal {
    
    private LSMDBWorker[]                 worker;
    
    /**
     * the disk logger is used to write InsertRecordGroups persistently to disk
     */
    private DiskLogger                    logger;
    
    private TransactionManagerInternal    txnMan;
    
    /**
     * Checkpointer thread for automatic checkpointing
     */
    private final CheckpointerInternal    dbCheckptr;
    
    /**
     * the component that manages database snapshots
     */
    private final SnapshotManagerInternal snapshotManager;
    
    /**
     * the component that manages databases
     */
    private final DatabaseManagerInternal databaseManager;
    
    /**
     * the thread managing user-response listeners
     */
    private final ResponseManagerImpl     responseManager;
    
    /**
     * All necessary parameters to run the BabuDB.
     */
    private final BabuDBConfig            configuration;
    
    /**
     * File used to store meta-informations about DBs.
     */
    private final DBConfig                dbConfigFile;
    
    /**
     * Flag that shows the replication if babuDB is running at the moment.
     */
    private final AtomicBoolean           stopped = new AtomicBoolean(true);
    
    /**
     * Threads used within the plugins. They will be stopped when BabuDB shuts
     * down.
     */
    private final List<LifeCycleThread>   plugins = new Vector<LifeCycleThread>();
    
    static {
        
        // BufferPool.enableStackTraceRecording(true);
        
        // Check if the correct version of the foundation JAR is available. If
        // this is not the case, an error is thrown immediately on startup.
        // Whenever a new foundation JAR is imported, the required version
        // number should be adjusted, so as to make sure that BabuDB behaves as
        // expected.
        final int requiredFoundationVersion = 2;
        
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
        this.responseManager = new ResponseManagerImpl(configuration.getMaxQueueLength());
        this.txnMan = new TransactionManagerImpl(
                configuration.getSyncMode().equals(SyncMode.ASYNC));
        this.databaseManager = new DatabaseManagerImpl(this);
        this.dbConfigFile = new DBConfig(this);
        this.snapshotManager = new SnapshotManagerImpl(this);
        this.dbCheckptr = new CheckpointerImpl(this);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInternal#init(
     * org.xtreemfs.babudb.StaticInitialization)
     */
    @Override
    public void init(StaticInitialization staticInit) throws BabuDBException {
        
        responseManager.setLifeCycleListener(this);
        responseManager.start();
        snapshotManager.init();
        
        // determine the LSN from which to start the log replay
        
        // to be able to recover from crashes during checkpoints, it is
        // necessary to start with the smallest LSN found on disk
        LSN dbLsn = null;
        for (DatabaseInternal db : databaseManager.getDatabaseList()) {
            if (dbLsn == null)
                dbLsn = db.getLSMDB().getOndiskLSN();
            else {
                LSN onDiskLSN = db.getLSMDB().getOndiskLSN();
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
            logger = new DiskLogger(configuration.getDbLogDir(), nextLSN, 
                    configuration.getSyncMode(), configuration.getPseudoSyncWait(),
                    configuration.getMaxQueueLength() * Math.max(1, configuration.getNumThreads()));
            logger.setLifeCycleListener(this);
            logger.start();
            logger.waitForStartup();
        } catch (Exception ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
        }
        this.txnMan.init(new LSN(nextLSN.getViewId(), nextLSN.getSequenceNo() - 1L));
        this.txnMan.setLogger(logger);
        
        if (configuration.getNumThreads() > 0) {
            worker = new LSMDBWorker[configuration.getNumThreads()];
            for (int i = 0; i < configuration.getNumThreads(); i++) {
                worker[i] = new LSMDBWorker(this, i, configuration.getMaxQueueLength());
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
        
        final LSN firstLSN = new LSN(1, 1L);
        if (staticInit != null && nextLSN.equals(firstLSN)) {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "Running initialization script...");
            staticInit.initialize(databaseManager, snapshotManager);
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "... initialization script finished successfully.");
        } else if (staticInit != null) {
            Logging.logMessage(Logging.LEVEL_INFO, this, "Static initialization was ignored, "
                + "because database is not empty.");
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
                dbCheckptr.suspendCheckpointing();
                logger.waitForShutdown();
                txnMan.setLogger(null);
                if (worker != null)
                    for (LSMDBWorker w : worker)
                        w.waitForShutdown();
                
            } catch (Exception ex) {
                Logging.logMessage(Logging.LEVEL_ERROR, this, "BabuDB could"
                    + " not be stopped, because '%s'.", ex.getMessage());
            }
            stopped.set(true);
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
            for (DatabaseInternal db : databaseManager.getDatabaseList()) {
                if (dbLsn == null)
                    dbLsn = db.getLSMDB().getOndiskLSN();
                else {
                    LSN onDiskLSN = db.getLSMDB().getOndiskLSN();
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
                logger = new DiskLogger(configuration.getDbLogDir(), nextLSN, 
                        configuration.getSyncMode(), configuration.getPseudoSyncWait(),
                        configuration.getMaxQueueLength() * configuration.getNumThreads());
                logger.setLifeCycleListener(this);
                logger.start();
                logger.waitForStartup();
            } catch (Exception ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR,
                    "Cannot start " + "database operations logger!", ex);
            }
            this.txnMan.init(new LSN(nextLSN.getViewId(), nextLSN.getSequenceNo() - 1L));
            this.txnMan.setLogger(logger);
            
            if (configuration.getNumThreads() > 0) {
                worker = new LSMDBWorker[configuration.getNumThreads()];
                for (int i = 0; i < configuration.getNumThreads(); i++) {
                    worker[i] = new LSMDBWorker(this, i, configuration.getMaxQueueLength());
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
    
    @Override
    public void shutdown() throws BabuDBException {
        shutdown(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#shutdown(boolean graceful)
     */
    @Override
    public void shutdown(boolean graceful) throws BabuDBException {
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "shutting down ...");
        
        if (worker != null) {
            for (LSMDBWorker w : worker) {
                w.shutdown(graceful);
            }
        }
        
        // stop the plugin threads
        BabuDBException exc = null;
        for (LifeCycleThread p : plugins) {
            try {
                p.shutdown();
                p.waitForShutdown();
            } catch (Exception e) {
                if (exc == null)
                    exc = new BabuDBException(ErrorCode.BROKEN_PLUGIN, e.getMessage(), e.getCause());
            }
        }
        
        // shut down the logger; this keeps insertions from being completed
        try {
            logger.shutdown(graceful);
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_DEBUG, this, e);
        }
        
        // complete checkpoint before shutdown
        try {
            dbCheckptr.shutdown();
            dbCheckptr.waitForShutdown();
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_DEBUG, this, e);
        }
        
        try {
            databaseManager.shutdown();
            snapshotManager.shutdown();
        } catch (Exception e) {
            Logging.logError(Logging.LEVEL_DEBUG, this, e);
        }
        
        if (worker != null) {
            for (LSMDBWorker w : worker) {
                try {
                    w.waitForShutdown();
                } catch (Exception e) {
                    Logging.logError(Logging.LEVEL_DEBUG, this, e);
                }
            }
            
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "%d worker threads shut down " +
            		"successfully", worker.length);
        }
        
        try {
            responseManager.shutdown();
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.INTERRUPTED, e.getMessage(), e);
        }
        
        if (exc != null) {
            throw exc;
        }
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB shutdown complete.");
    }
    
    /**
     * NEVER USE THIS EXCEPT FOR UNIT TESTS! Kills the database.
     */
    @SuppressWarnings("deprecation")
    public void __test_killDB_dangerous() {
        try {
            logger.destroy();
            if (worker != null)
                for (LSMDBWorker w : worker)
                    w.stop();
            ((Thread) dbCheckptr).stop();
            
            this.databaseManager.shutdown();
            this.snapshotManager.shutdown();
            
        } catch (Exception ex) {
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
    public CheckpointerInternal getCheckpointer() {
        return dbCheckptr;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.xtreemfs.babudb.api.dev.BabuDBInternal#replaceTransactionManager(
     * org.xtreemfs.babudb.api.dev.transaction.TransactionManagerInternal)
     */
    @Override
    public void replaceTransactionManager(TransactionManagerInternal txnMan) {
        this.txnMan = txnMan;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.dev.BabuDBInternal#getTransactionManager()
     */
    @Override
    public TransactionManagerInternal getTransactionManager() {
        return txnMan;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.api.BabuDB#getDatabaseManager()
     */
    @Override
    public DatabaseManagerInternal getDatabaseManager() {
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
    public SnapshotManagerInternal getSnapshotManager() {
        return snapshotManager;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.BabuDBInternal#getResponseManager()
     */
    @Override
    public ResponseManagerInternal getResponseManager() {
        return responseManager;
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
        if (worker == null) {
            return null;
        }
        return worker[dbId % worker.length];
    }
    
    /**
     * Replays the database operations log.
     * 
     * @param from
     *            - LSN to replay the logs from.
     * 
     * @return the LSN to assign to the next operation
     * 
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
                    byte type = le.getPayloadType();
                    
                    Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                            "Reading entry LSN(%s) of type (%d) with %d bytes payload from log.", 
                            le.getLSN().toString(), (int) type, le.getPayload().remaining());
                    
                    // in normal there are only transactions to be replayed
                    if (type == PAYLOAD_TYPE_TRANSACTION) {
                        txnMan.replayTransaction(le);
                        
                        // create, copy and delete are not replayed (this block
                        // is for backward
                        // compatibility)
                    } else if (type != PAYLOAD_TYPE_CREATE && type != PAYLOAD_TYPE_COPY
                        && type != PAYLOAD_TYPE_DELETE) {
                        
                        // get the processing logic for the dedicated logEntry
                        // type
                        InMemoryProcessing processingLogic = txnMan.getProcessingLogic().get(type);
                        
                        // deserialize the arguments retrieved from the logEntry
                        OperationInternal operation = processingLogic.convertToOperation(
                                processingLogic.deserializeRequest(le.getPayload()));
                        
                        // execute the in-memory logic
                        try {
                            processingLogic.process(operation);
                        } catch (BabuDBException be) {
                            
                            // there might be false positives if a snapshot to
                            // delete has already
                            // been deleted or a snapshot to create has already
                            // been created
                            if (!(type == PAYLOAD_TYPE_SNAP && 
                                     be.getErrorCode() == ErrorCode.SNAP_EXISTS)
                             && !(type == PAYLOAD_TYPE_SNAP_DELETE && 
                                     be.getErrorCode() == ErrorCode.NO_SUCH_SNAPSHOT)) {
                                
                                throw be;
                            }
                        }
                    }
                    
                    // set LSN
                    nextLSN = new LSN(le.getViewId(), le.getLogSequenceNo() + 1L);
                } finally {
                    if (le != null) {
                        le.free();
                    }
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
            
            Logging.logError(Logging.LEVEL_ERROR, this, ex);
            
            if (ex.getCause() instanceof LogEntryException) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "corrupted/incomplete log entry in " +
                		"database operations log", ex.getCause());
            } else {
                throw new BabuDBException(ErrorCode.IO_ERROR, "corrupted/incomplete log entry in " +
                		"database operations log", ex);
            }
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
    public void startupPerformed() { 
        Logging.logMessage(Logging.LEVEL_INFO, this, "has been successfully started.");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() { 
        Logging.logMessage(Logging.LEVEL_INFO, this, "has been successfully stopped.");
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
            Logging.logMessage(Logging.LEVEL_WARN, this, "BabuDB could not have been terminated " +
            		"gracefully after plugin crash. " + "Because: %s", e.getMessage());
            e.printStackTrace();
        }
    }
}