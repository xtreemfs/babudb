/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import static org.xtreemfs.babudb.config.ReplicationConfig.slaveProtection;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.conversion.AutoConverter;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.Checkpointer;
import org.xtreemfs.babudb.lsmdb.CheckpointerImpl;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.DirectFileIO;
import org.xtreemfs.babudb.replication.ReplicationManager;
import org.xtreemfs.babudb.replication.ReplicationManagerImpl;
import org.xtreemfs.babudb.replication.stages.logic.LoadLogic;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.babudb.snapshots.SnapshotManager;
import org.xtreemfs.babudb.snapshots.SnapshotManagerImpl;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.common.logging.Logging.Category;

/**
 * <p>
 * <b>Please use the {@link BabuDBFactory} for retrieving an instance of
 * {@link BabuDB}.</b>
 * </p>
 * 
 * @author bjko
 * @author flangner
 * 
 */
public class BabuDB {
    
    /**
     * Version (name)
     */
    public static final String           BABUDB_VERSION           = "0.3.3";
    
    /**
     * Version of the DB on-disk format (to detect incompatibilities).
     */
    public static final int              BABUDB_DB_FORMAT_VERSION = 4;
    
    /**
     * the disk logger is used to write InsertRecordGroups persistently to disk
     */
    private DiskLogger                   logger;
    
    private LSMDBWorker[]                worker;
    
    private final ReplicationManagerImpl replicationManager;
    
    /**
     * Checkpointer thread for automatic checkpointing
     */
    private CheckpointerImpl             dbCheckptr;
    
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
    private final AtomicBoolean          stopped;
    
    /**
     * Flag that determines if the slaveCheck should be enabled, or not.
     */
    private volatile boolean             slaveCheck               = true;
    
    /**
     * Starts the BabuDB database. If conf is instance of MasterConfig it comes
     * with replication in master-mode. If conf is instance of SlaveConfig it
     * comes with replication in slave-mode.
     * 
     * @param conf
     * @throws BabuDBException
     */
    BabuDB(BabuDBConfig conf) throws BabuDBException {
        Logging.start(conf.getDebugLevel());
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "base dir: " + conf.getBaseDir());
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "db log dir: " + conf.getDbLogDir());
        
        this.configuration = conf;
        this.databaseManager = new DatabaseManagerImpl(this);
        this.dbConfigFile = new DBConfig(this);
        this.snapshotManager = new SnapshotManagerImpl(this);
        this.dbCheckptr = new CheckpointerImpl(this);
        
        if (dbConfigFile.isConversionRequired()) {
            
            Logging
                    .logMessage(
                        Logging.LEVEL_WARN,
                        Category.db,
                        this,
                        "The database version is outdated. The database will be automatically converted to the latest version if possible. This may take some time, depending on the size.");
            
            AutoConverter.initiateConversion(dbConfigFile.getDBFormatVersion(), conf);
        }
        
        snapshotManager.init();
        
        try {
            if (conf instanceof ReplicationConfig)
                DirectFileIO.replayBackupFiles((ReplicationConfig) conf);
        } catch (IOException io) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "Could not retrieve the "
                + "slave backup files, because: ", io.getMessage());
        }
        // determine the last LSN and replay the log
        LSN dbLsn = null;
        for (Database db : databaseManager.getDatabaseList()) {
            if (dbLsn == null)
                dbLsn = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
            else {
                LSN onDiskLSN = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
                if (!(LSMDatabase.NO_DB_LSN.equals(dbLsn) || LSMDatabase.NO_DB_LSN.equals(onDiskLSN)) && !dbLsn.equals(onDiskLSN))
                    throw new RuntimeException("databases have different LSNs: current LSN=" + onDiskLSN + ", LSN seen before=" + dbLsn);
            }
        }
        if (dbLsn == null) {
            // empty babudb
            dbLsn = new LSN(0, 0);
        } else {
            // need next LSN which is onDisk + 1
            dbLsn = new LSN(dbLsn.getViewId(), dbLsn.getSequenceNo() + 1);
        }
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "starting log replay");
        LSN nextLSN = replayLogs(dbLsn);
        if (dbLsn.compareTo(nextLSN) > 0) {
            nextLSN = dbLsn;
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done, using LSN: " + nextLSN);
        
        // set up the replication service
        LSN lastLSN = new LSN(nextLSN.getViewId(), nextLSN.getSequenceNo() - 1);
        try {
            if (conf instanceof ReplicationConfig)
                this.replicationManager = new ReplicationManagerImpl(this, lastLSN);
            else
                this.replicationManager = null;
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        }
        
        // set up and start the disk logger
        try {
            this.logger = new DiskLogger(conf.getDbLogDir(), nextLSN.getViewId(), nextLSN.getSequenceNo(),
                conf.getSyncMode(), conf.getPseudoSyncWait(),
                conf.getMaxQueueLength() * conf.getNumThreads(), replicationManager);
            this.logger.start();
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
        }
        
        if (conf.getNumThreads() > 0) {
            worker = new LSMDBWorker[conf.getNumThreads()];
            for (int i = 0; i < conf.getNumThreads(); i++) {
                worker[i] = new LSMDBWorker(logger, i, (conf.getPseudoSyncWait() > 0), conf
                        .getMaxQueueLength());
                worker[i].start();
            }
        } else {
            // number of workers is 0 => requests will be responded directly.
            assert (conf.getNumThreads() == 0);
            
            worker = null;
        }
        
        if (dbConfigFile.isConversionRequired())
            AutoConverter.completeConversion(this);
        
        // initialize and start the checkpointer; this has to be separated from
        // the instantiation because the instance has to be there when the log
        // is replayed
        dbCheckptr.init(logger, conf.getCheckInterval(), conf.getMaxLogfileSize());
        dbCheckptr.start();
        
        // start the replication service after all other components of babuDB
        // have been started successfully
        this.stopped = new AtomicBoolean(false);
        if (this.replicationManager != null)
            this.replicationManager.initialize();
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is running " + "(version "
            + BABUDB_VERSION + ")");
    }
    
    /**
     * REPLICATION Stops the BabuDB for an initial Load. This is necessary if
     * the replication has copied the onDiskData from a master- participant due
     * a {@link LoadLogic} run.
     */
    public void stop() {
        synchronized (stopped) {
            if (stopped.get())
                return;
            
            if (worker != null)
                for (LSMDBWorker w : worker)
                    w.shutdown();
            
            logger.shutdown();
            dbCheckptr.shutdown();
            try {
                logger.waitForShutdown();
                if (worker != null)
                    for (LSMDBWorker w : worker)
                        w.waitForShutdown();
                
                dbCheckptr.waitForShutdown();
            } catch (InterruptedException ex) { /* ignored */
            }
            this.stopped.set(true);
            Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB has been stopped by the Replication.");
            
        }
    }
    
    /**
     * <p>
     * Needed for the initial load process of the babuDB, done by the
     * replication.
     * </p>
     * 
     * @throws BabuDBException
     * @return the next LSN.
     */
    public LSN restart() throws BabuDBException {
        synchronized (stopped) {
            if (!stopped.get())
                throw new BabuDBException(ErrorCode.IO_ERROR, "BabuDB has to be stopped before!");
            
            databaseManager.reset();
            
            dbCheckptr = new CheckpointerImpl(this);
            
            LSN dbLsn = null;
            LSN zero = new LSN(0, 0L);
            for (Database dbRaw : databaseManager.getDatabaseList()) {
                DatabaseImpl db = (DatabaseImpl) dbRaw;
                LSN onDisk = db.getLSMDB().getOndiskLSN();
                if (dbLsn == null && !onDisk.equals(zero))
                    dbLsn = onDisk;
                else if (dbLsn != null) {
                    if (!onDisk.equals(zero) && !dbLsn.equals(onDisk))
                        throw new RuntimeException("databases have different LSNs! " + dbLsn.toString()
                            + " != " + db.getLSMDB().getOndiskLSN().toString());
                }
            }
            if (dbLsn == null) {
                // empty babudb
                dbLsn = new LSN(0, 0);
            } else {
                // need next LSN which is onDisk + 1
                dbLsn = new LSN(dbLsn.getViewId(), dbLsn.getSequenceNo() + 1);
            }
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "starting log replay");
            LSN nextLSN = replayLogs(dbLsn);
            if (dbLsn.compareTo(nextLSN) > 0)
                nextLSN = dbLsn;
            Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done, using LSN: " + nextLSN);
            
            try {
                logger = new DiskLogger(configuration.getDbLogDir(), nextLSN.getViewId(), nextLSN
                        .getSequenceNo(), configuration.getSyncMode(), configuration.getPseudoSyncWait(),
                    configuration.getMaxQueueLength() * configuration.getNumThreads(), replicationManager);
                logger.start();
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
            }
            
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
            
            dbCheckptr.init(logger, configuration.getCheckInterval(), configuration.getMaxLogfileSize());
            dbCheckptr.start();
            
            Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is " + "running (version "
                + BABUDB_VERSION + ")");
            
            this.stopped.set(false);
            return new LSN(nextLSN.getViewId(), nextLSN.getSequenceNo() - 1L);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInterface#shutdown()
     */
    public void shutdown() throws BabuDBException {
        if (worker != null)
            for (LSMDBWorker w : worker)
                w.shutdown();
        
        // stop the replication
        if (replicationManager != null) {
            replicationManager.shutdown();
        }
        
        logger.shutdown();
        dbCheckptr.shutdown();
        databaseManager.shutdown();
        snapshotManager.shutdown();
        
        try {
            logger.waitForShutdown();
            if (worker != null)
                for (LSMDBWorker w : worker)
                    w.waitForShutdown();
            
            dbCheckptr.waitForShutdown();
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
            
        } catch (IllegalMonitorStateException ex) {
            // we will probably get that when we kill a thread because we do
            // evil stuff here ;-)
        }
    }
    
    public Checkpointer getCheckpointer() {
        return dbCheckptr;
    }
    
    public DiskLogger getLogger() {
        return logger;
    }
    
    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }
    
    public DatabaseManager getDatabaseManager() {
        return databaseManager;
    }
    
    public BabuDBConfig getConfig() {
        return configuration;
    }
    
    /**
     * @return the path to the DB-configuration-file, if available, null
     *         otherwise.
     */
    public String getDBConfigPath() {
        String result = configuration.getBaseDir() + configuration.getDbCfgFile();
        File f = new File(result);
        if (f.exists())
            return result;
        return null;
    }
    
    /**
     * Replay the database operations log.
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
                            if(db == null)
                                break;
                            
                            snapshotManager.createPersistentSnapshot(db.getName(),
                                snap, false);
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
                    if (le!=null) le.free();
                }
                
            }
            
            if (nextLSN != null) {
                return nextLSN;
            } else {
                return new LSN(1, 1);
            }
            
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "cannot load database operations log, file might be corrupted", ex);
        } catch (Exception ex) {
            if (ex.getCause() instanceof LogEntryException) {
                throw new BabuDBException(ErrorCode.IO_ERROR,
                    "corrupted/incomplete log entry in database operations log", ex.getCause());
            } else
                throw new BabuDBException(ErrorCode.IO_ERROR,
                    "corrupted/incomplete log entry in database operations log", ex);
        }
    }
    
    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }
    
    public DBConfig getDBConfigFile() {
        return dbConfigFile;
    }
    
    /**
     * Returns the number of worker threads.
     * 
     * @return the number of worker threads.
     */
    public int getWorkerCount() {
        if (worker == null)
            return 0;
        return worker.length;
    }
    
    /**
     * 
     * @param dbId
     * @return a worker Thread, responsible for the DB given by its ID.
     */
    public LSMDBWorker getWorker(int dbId) {
        if (worker == null)
            return null;
        return worker[dbId % worker.length];
    }
    
    public void enableSlaveCheck() {
        this.slaveCheck = true;
    }
    
    public void disableSlaveCheck() {
        this.slaveCheck = false;
    }
    
    /**
     * 
     * @return true, if replication runs in slave-mode, false otherwise.
     */
    public void slaveCheck() throws BabuDBException {
        if (slaveCheck && replicationManager != null && !replicationManager.isMaster())
            throw new BabuDBException(ErrorCode.NO_ACCESS, slaveProtection);
    }
}
