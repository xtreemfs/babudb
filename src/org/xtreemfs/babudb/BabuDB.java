/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.log.DiskLogFile;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.Checkpointer;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseImpl;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.replication.BabuDBReplication;
import org.xtreemfs.babudb.snapshots.SnapshotManager;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.config.BabuDBConfig;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.config.SlaveConfig;
import org.xtreemfs.include.common.logging.Logging;

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
    public static final String      BABUDB_VERSION           = "0.2.0";
    
    /**
     * Version of the DB on-disk format (to detect incompatibilities).
     */
    public static final int         BABUDB_DB_FORMAT_VERSION = 1;
    
    /**
     * the disk logger is used to write InsertRecordGroups persistently to disk
     * - visibility changed to public, because the replication needs access to
     * the {@link DiskLogger}
     */
    public DiskLogger               logger;
    
    /**
     * object used for locking to ensure that switch of log file and overlay
     * trees is atomic and not interrupted by inserts
     */
    public ReadWriteLock            overlaySwitchLock;
    
    private LSMDBWorker[]           worker;
    
    private final BabuDBReplication replication;
    
    /**
     * Checkpointer thread for automatic checkpointing -has to be public for
     * replication issues
     */
    public Checkpointer             dbCheckptr;
    
    /**
     * the component that manages database snapshots
     */
    private final SnapshotManager   snapshotManager;
    
    /**
     * the component that manages databases
     */
    private final DatabaseManager   databaseManager;
    
    /**
     * All necessary parameters to run the BabuDB.
     */
    public final BabuDBConfig       configuration;
    
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
        
        this.overlaySwitchLock = new ReentrantReadWriteLock();
        this.configuration = conf;
        
        this.databaseManager = new DatabaseManager(this);
        this.snapshotManager = new SnapshotManager(this);
        
        // determine the last LSN and replay the log
        LSN dbLsn = null;
        for (Database db : databaseManager.getDatabases()) {
            if (dbLsn == null)
                dbLsn = ((DatabaseImpl) db).getLSMDB().getOndiskLSN();
            else {
                if (!dbLsn.equals(((DatabaseImpl) db).getLSMDB().getOndiskLSN()))
                    throw new RuntimeException("databases have different LSNs!");
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
        LSN nextLSN = replayLogs();
        if (dbLsn.compareTo(nextLSN) > 0) {
            nextLSN = dbLsn;
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done, using LSN: " + nextLSN);
        
        // setup the replication service
        try {
            if (conf instanceof MasterConfig)
                this.replication = new BabuDBReplication((MasterConfig) conf, this, nextLSN);
            else if (conf instanceof SlaveConfig)
                this.replication = new BabuDBReplication((SlaveConfig) conf, this, nextLSN);
            else
                this.replication = null;
        } catch (IOException e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, e.getMessage());
        }
        
        // start the replication service
        if (this.replication != null)
            this.replication.initialize();
        
        // set up and start the disk logger
        try {
            this.logger = new DiskLogger(conf.getDbLogDir(), nextLSN.getViewId(), nextLSN.getSequenceNo(),
                conf.getSyncMode(), conf.getPseudoSyncWait(), conf.getMaxQueueLength() * conf.getNumThreads());
            this.logger.start();
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
        }
        
        worker = new LSMDBWorker[conf.getNumThreads()];
        for (int i = 0; i < conf.getNumThreads(); i++) {
            worker[i] = new LSMDBWorker(logger, i, overlaySwitchLock, (conf.getPseudoSyncWait() > 0), conf
                    .getMaxQueueLength(), replication);
            worker[i].start();
        }
        
        dbCheckptr = new Checkpointer(this, logger, conf.getCheckInterval(), conf.getMaxLogfileSize());
        dbCheckptr.start();
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is running (version " + BABUDB_VERSION
            + ")");
    }
    
    /**
     * DUMMY - constructor for testing only!
     */
    public BabuDB() {
        replication = null;
        configuration = null;
        snapshotManager = null;
        databaseManager = null;
    }
    
    /**
     * <p>
     * Needed for the initial load process of the babuDB, done by the
     * replication.
     * </p>
     * 
     * @param latest
     *            - {@link LSN} until which the loading is done.
     * @throws BabuDBException
     */
    public void reset(LSN latest) throws BabuDBException {
        for (LSMDBWorker w : worker) {
            w.shutdown();
        }
        logger.shutdown();
        dbCheckptr.shutdown();
        
        try {
            logger.waitForShutdown();
            for (LSMDBWorker w : worker) {
                w.waitForShutdown();
            }
            if (dbCheckptr != null) {
                dbCheckptr.waitForShutdown();
            }
        } catch (InterruptedException ex) {
        }
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB has been stopped by the Replication.");
        
        databaseManager.reset();
        
        try {
            logger = new DiskLogger(configuration.getDbLogDir(), latest.getViewId(),
                latest.getSequenceNo() + 1L, configuration.getSyncMode(), configuration.getPseudoSyncWait(),
                configuration.getMaxQueueLength() * configuration.getNumThreads());
            logger.start();
        } catch (IOException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
        }
        
        overlaySwitchLock = new ReentrantReadWriteLock();
        
        worker = new LSMDBWorker[configuration.getNumThreads()];
        for (int i = 0; i < configuration.getNumThreads(); i++) {
            worker[i] = new LSMDBWorker(logger, i, overlaySwitchLock,
                (configuration.getPseudoSyncWait() > 0), configuration.getMaxQueueLength(), replication);
            worker[i].start();
        }
        
        dbCheckptr = new Checkpointer(this, logger, configuration.getCheckInterval(), configuration
                .getMaxLogfileSize());
        dbCheckptr.start();
        
        Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is running (version " + BABUDB_VERSION
            + ")");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.xtreemfs.babudb.BabuDBInterface#shutdown()
     */
    public void shutdown() throws BabuDBException {
        for (LSMDBWorker w : worker) {
            w.shutdown();
        }
        
        // stop the replication
        if (replication != null) {
            replication.shutdown();
        }
        
        logger.shutdown();
        dbCheckptr.shutdown();
        databaseManager.shutdown();
        snapshotManager.shutdown();
        
        try {
            logger.waitForShutdown();
            for (LSMDBWorker w : worker) {
                w.waitForShutdown();
            }
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
            for (LSMDBWorker w : worker) {
                w.stop();
            }
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
    
    public BabuDBReplication getReplicationManager() {
        return replication;
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
     * @return the LSN to assign to the next operation
     * @throws BabuDBException
     */
    private LSN replayLogs() throws BabuDBException {
        if (databaseManager.getDatabases().size() == 0) {
            return new LSN(1, 1);
        }
        
        try {
            File f = new File(configuration.getDbLogDir());
            String[] logs = f.list(new FilenameFilter() {
                
                public boolean accept(File dir, String name) {
                    return name.endsWith(".dbl");
                }
            });
            LSN nextLSN = null;
            if (logs != null) {
                // read list of logs and create a list ordered from min LSN to
                // max LSN
                SortedSet<LSN> orderedLogList = new TreeSet<LSN>();
                Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
                for (String log : logs) {
                    Matcher m = p.matcher(log);
                    m.matches();
                    String tmp = m.group(1);
                    int viewId = Integer.valueOf(tmp);
                    tmp = m.group(2);
                    int seqNo = Integer.valueOf(tmp);
                    orderedLogList.add(new LSN(viewId, seqNo));
                }
                // apply log entries to databases...
                for (LSN logLSN : orderedLogList) {
                    DiskLogFile dlf = new DiskLogFile(configuration.getDbLogDir(), logLSN);
                    LogEntry le = null;
                    while (dlf.hasNext()) {
                        le = dlf.next();
                        // do something
                        ReusableBuffer payload = le.getPayload();
                        if (payload.array().length != 0) {
                            InsertRecordGroup ai = InsertRecordGroup.deserialize(payload);
                            insert(ai);
                        }
                        le.free();
                    }
                    // set lsn'
                    if (le != null) {
                        nextLSN = new LSN(le.getViewId(), le.getLogSequenceNo() + 1);
                    }
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
        } catch (LogEntryException ex) {
            throw new BabuDBException(ErrorCode.IO_ERROR,
                "corrupted/incomplete log entry in database operations log", ex);
        } catch (Exception ex) {
            throw new BabuDBException(ErrorCode.INTERNAL_ERROR,
                "cannot load database operations log, unexpected error", ex);
        }
        
    }
    
    /**
     * Insert a full record group. Only to be used by log replay.
     * 
     * @param ins
     */
    private void insert(InsertRecordGroup ins) {
        final LSMDatabase db = ((DatabaseImpl) databaseManager.getDatabase(ins.getDatabaseId())).getLSMDB();
        // ignore deleted databases when recovering!
        if (db == null) {
            return;
        }
        
        for (InsertRecord ir : ins.getInserts()) {
            LSMTree tree = db.getIndex(ir.getIndexId());
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "insert " + new String(ir.getKey()) + "="
                + (ir.getValue() == null ? null : new String(ir.getValue())) + " into "
                + db.getDatabaseName() + " " + ir.getIndexId());
            tree.insert(ir.getKey(), ir.getValue());
        }
        
    }
    
    /**
     * FOR TESTING PURPOSE ONLY!
     * 
     * @param databaseName the database name
     * @param indexId the ID of the index
     * @param key the key
     * @return the value
     * @throws BabuDBException
     */
    public byte[] hiddenLookup(String databaseName, int indexId, byte[] key) throws BabuDBException {
        final LSMDatabase db = ((DatabaseImpl) databaseManager.getDatabase(databaseName)).getLSMDB();
        
        if (db == null) {
            throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
        }
        if ((indexId >= db.getIndexCount()) || (indexId < 0)) {
            throw new BabuDBException(ErrorCode.NO_SUCH_INDEX, "index does not exist");
        }
        return db.getIndex(indexId).lookup(key);
    }
    
    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }
    
    /**
     * Returns the number of worker threads.
     * 
     * @return the number of worker threads.
     */
    public int getWorkerCount() {
        return worker.length;
    }
    
    /**
     * 
     * @param dbId
     * @return a worker Thread, responsible for the DB given by its ID.
     */
    public LSMDBWorker getWorker(int dbId) {
        return worker[dbId % worker.length];
    }
    
    /**
     * 
     * @return true, if replication runs in slave-mode, false otherwise.
     */
    public boolean replication_isSlave() {
        if (replication == null) {
            return false;
        }
        return !replication.isMaster();
    }
    
    /**
     * <p>
     * Dangerous function. Just for testing purpose!
     * </p>
     * 
     * @return the LSN of the last written insert.
     * @throws InterruptedException
     * 
     *             public LSN replication_pause() throws InterruptedException{
     *             return replication.pause(); }
     */
    
    /**
     * <p>
     * Dangerous function. Just for testing purpose!
     * </p>
     * 
     * public void replication_resume(){ replication.resume(); }
     * */
    
    /**
     * <p>
     * Makes a new checkPoint and increments the viewID.
     * </p>
     * 
     * @throws BabuDBException
     * @throws InterruptedException
     * 
     *             public void replication_toMaster() throws
     *             InterruptedException, BabuDBException{ assert
     *             (replication!=null); checkpoint(true);
     *             replication.setSlaves(configuration.replication_slaves); }
     */
    
    /**
     * <p>
     * Operation to switch the synchronization policy while replication is
     * running.
     * </p>
     * 
     * @see {@link Replication}.setSyncMode(int)
     * @param n
     * @throws BabuDBException
     * @throws InterruptedException
     * 
     *             public void replication_switchSyncMode(int n) throws
     *             BabuDBException, InterruptedException{ if (replication !=
     *             null) replication.setSyncMode(n); else throw new
     *             UnsupportedOperationException ("Replication is not enabled! That's why it does not make sense to change the replication policies."
     *             ); }
     */
    
    /**
     * TODO failover-strategy
     * 
     * @param msg
     * 
     *            public void replication_runtime_failure(String msg) {
     * 
     *            String message = "BabuDB in "+((replication_isSlave()) ?
     *            "slave" : "master")+"-mode has failed: "+msg;
     *            Logging.logMessage(Logging.LEVEL_ERROR, this, message);
     *            shutdown(); }
     */
    
}
