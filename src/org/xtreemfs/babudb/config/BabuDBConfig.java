/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.babudb.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Reading configurations from the babuDB-config-file.
 * 
 * @since 06/05/2009
 * @author flangner
 * 
 */

public class BabuDBConfig extends Config {
    
    /** Log Level. */
    protected int      debugLevel;
    
    protected String   debugCategory;
    
    /**
     * The database configuration file. Contains the names and number of indices
     * for the databases.
     */
    protected String   dbCfgFile;
    
    /**
     * Base directory to store database index snapshots in.
     */
    protected String   baseDir;
    
    /**
     * Directory in which the database logs are stored.
     */
    protected String   dbLogDir;
    
    /**
     * SyncMode the synchronization mode to use for the logFile.
     */
    protected SyncMode syncMode;
    
    /**
     * Max queue length: if > 0, the queue for each worker is limited to maxQ.
     * The replication remote-request-queue will also be limited to maxQ.
     */
    protected int      maxQueueLength;
    
    /**
     * NumThreads number of worker threads to use.
     */
    protected int      numThreads;
    
    /**
     * MaxLogfileSize a checkpoint is generated ,if maxLogfileSize is exceeded.
     */
    protected long     maxLogfileSize;
    
    /**
     * CheckInterval interval between two checks in seconds, 0 disables auto
     * checkPointing.
     */
    protected int      checkInterval;
    
    /**
     * If set to a value > 0, operations are acknowledges immediately before
     * they are written to the disk log. The disk logger will do batch writes
     * and call fSync... every pseudoSyncWait seconds. This can be used to
     * increase performance and emulate PostgreSQL behavior.
     */
    protected int      pseudoSyncWait;
    
    /**
     * Indicates if compression is enabled or not.
     */
    protected boolean  compression;
    
    /**
     * Defines the maximum number of records per block of an index.
     */
    protected int      maxNumRecordsPerBlock;
    
    /**
     * Defines the maximum size of the block file. If the size is exceeded by an
     * index, another block file will be created.
     */
    protected int      maxBlockFileSize;
    
    /**
     * Crates a new BabuDB configuration.
     * 
     * @param dbDir
     *            the directory in which persistent checkpoints are stored
     * @param dbLogDir
     *            the directory in which the database log resides
     * @param numThreads
     *            the number of worker threads for request processing; if set to
     *            0, requests are processed in the context of the invoking
     *            thread
     * @param maxLogFileSize
     *            the maximum file size for the log; if exceeded, a new
     *            checkpoint will be created and the log will be truncated
     * @param checkInterval
     *            the frequency at which checks are performed if the log file
     *            size is exceeded
     * @param syncMode
     *            the synchronization mode for log append writes
     * @param pseudoSyncWait
     *            the time for batching requests before writing them to disk
     *            (only relevant if SyncMode = SyncMode.ASYNC)
     * @param maxQ
     *            the maximum queue length for each worker thread as well as
     *            replication-related requests; if set to 0, no limit will be
     *            enforced on the queue length
     * @param compression
     *            specifies whether simple data compression is enabled
     * @param maxNumRecordsPerBlock
     *            defines the maximum number of records per block in the
     *            persistent block file of an index
     * @param maxBlockFileSize
     *            defines the maximum size of the persistent block file of an
     *            index; if exceeded, a new block file will be added
     */
    public BabuDBConfig(String dbDir, String dbLogDir, int numThreads, long maxLogFileSize,
        int checkInterval, SyncMode syncMode, int pseudoSyncWait, int maxQ, boolean compression,
        int maxNumRecordsPerBlock, int maxBlockFileSize) {
        
        checkArgs(dbDir, dbLogDir, numThreads, maxLogFileSize, checkInterval, syncMode, pseudoSyncWait, maxQ,
            compression, maxNumRecordsPerBlock, maxBlockFileSize);
        
        this.debugLevel = Logging.LEVEL_WARN;
        this.debugCategory = "all";
        this.baseDir = (dbDir.endsWith(File.separator)) ? dbDir : dbDir + File.separator;
        this.dbCfgFile = "config.db";
        this.dbLogDir = (dbLogDir.endsWith(File.separator)) ? dbLogDir : dbLogDir + File.separator;
        this.syncMode = syncMode;
        this.maxQueueLength = maxQ;
        this.numThreads = numThreads;
        this.checkInterval = checkInterval;
        this.pseudoSyncWait = pseudoSyncWait;
        this.maxLogfileSize = maxLogFileSize;
        this.compression = compression;
        this.maxNumRecordsPerBlock = maxNumRecordsPerBlock;
        this.maxBlockFileSize = maxBlockFileSize;
    }
    
    public BabuDBConfig(Properties prop) throws IOException {
        super(prop);
        read();
    }
    
    public BabuDBConfig(String filename) throws IOException {
        super(filename);
        read();
    }
    
    public BabuDBConfig copy() {
        return new BabuDBConfig(baseDir, dbLogDir, numThreads, maxLogfileSize, checkInterval, syncMode,
            pseudoSyncWait, maxQueueLength, compression, maxNumRecordsPerBlock, maxBlockFileSize);
    }
    
    /**
     * Writes out the config to the given filename.
     * 
     * @param filename
     * @throws IOException
     * @throws FileNotFoundException
     */
    public void dump(String filename) throws FileNotFoundException, IOException {
        this.write(filename);
    }
    
    public void read() throws IOException {
        
        this.debugLevel = readDebugLevel();
        
        this.debugCategory = this.readOptionalString("babudb.debug.category", "all");
        
        this.dbCfgFile = this.readOptionalString("babudb.cfgFile", "config.db");
        
        String baseDir = this.readRequiredString("babudb.baseDir");
        this.baseDir = (baseDir.endsWith(File.separator)) ? baseDir : baseDir + File.separator;
        
        String dbLogDir = this.readRequiredString("babudb.logDir");
        this.dbLogDir = (dbLogDir.endsWith(File.separator)) ? dbLogDir : dbLogDir + File.separator;
        
        this.syncMode = SyncMode.valueOf(this.readRequiredString("babudb.sync"));
        
        this.numThreads = this.readOptionalInt("babudb.worker.numThreads", 1);
        
        this.maxQueueLength = this.readOptionalInt("babudb.worker.maxQueueLength", 0);
        
        this.maxLogfileSize = this.readOptionalInt("babudb.maxLogfileSize", 1);
        
        this.checkInterval = this.readOptionalInt("babudb.checkInterval", 0);
        
        this.pseudoSyncWait = this.readOptionalInt("babudb.pseudoSyncWait", 0);
        
        this.compression = this.readOptionalBoolean("babudb.compression", false);
        
        this.maxNumRecordsPerBlock = this.readOptionalInt("babudb.maxNumRecordsPerBlock", 16);
        
        this.maxBlockFileSize = this.readOptionalInt("babudb.maxBlockFileSize", 1024 * 1024 * 512);
    }
    
    protected int readDebugLevel() {
        String level = props.getProperty("debug.level");
        if (level == null)
            return Logging.LEVEL_WARN;
        else {
            
            level = level.trim().toUpperCase();
            
            if (level.equals("EMERG")) {
                return Logging.LEVEL_EMERG;
            } else if (level.equals("ALERT")) {
                return Logging.LEVEL_ALERT;
            } else if (level.equals("CRIT")) {
                return Logging.LEVEL_CRIT;
            } else if (level.equals("ERR")) {
                return Logging.LEVEL_ERROR;
            } else if (level.equals("WARNING")) {
                return Logging.LEVEL_WARN;
            } else if (level.equals("NOTICE")) {
                return Logging.LEVEL_NOTICE;
            } else if (level.equals("INFO")) {
                return Logging.LEVEL_INFO;
            } else if (level.equals("DEBUG")) {
                return Logging.LEVEL_DEBUG;
            } else {
                
                try {
                    int levelInt = Integer.valueOf(level);
                    return levelInt;
                } catch (NumberFormatException ex) {
                    throw new RuntimeException("'" + level + "' is not a valid level name nor an integer");
                }
                
            }
            
        }
        
    }
    
    public int getDebugLevel() {
        return this.debugLevel;
    }
    
    public String getDebugCategory() {
        return this.debugCategory;
    }
    
    public String getDbCfgFile() {
        return dbCfgFile;
    }
    
    public String getBaseDir() {
        return baseDir;
    }
    
    public String getDbLogDir() {
        return dbLogDir;
    }
    
    public SyncMode getSyncMode() {
        return syncMode;
    }
    
    public int getMaxQueueLength() {
        return maxQueueLength;
    }
    
    public int getNumThreads() {
        return numThreads;
    }
    
    public long getMaxLogfileSize() {
        return maxLogfileSize;
    }
    
    public int getCheckInterval() {
        return checkInterval;
    }
    
    public int getPseudoSyncWait() {
        return pseudoSyncWait;
    }
    
    public boolean getCompression() {
        return compression;
    }
    
    public int getMaxNumRecordsPerBlock() {
        return maxNumRecordsPerBlock;
    }
    
    public int getMaxBlockFileSize() {
        return maxBlockFileSize;
    }
    
    private void checkArgs(String dbDir, String dbLogDir, int numThreads, long maxLogFileSize,
        int checkInterval, SyncMode syncMode, int pseudoSyncWait, int maxQ, boolean compression,
        int maxNumRecordsPerBlock, int maxBlockFileSize) {
        
        if (dbDir == null)
            throw new IllegalArgumentException("database directory needs to be specified!");
        
        if (dbLogDir == null)
            throw new IllegalArgumentException("database log directory needs to be specified!");
        
        if (numThreads < 0)
            throw new IllegalArgumentException("number of threads must be >= 0!");
        
        if (maxLogFileSize < 0)
            throw new IllegalArgumentException("max. log file size must be be >= 0!");
        
        if (checkInterval < 0)
            throw new IllegalArgumentException("check interval for log file size must be >= 0!");
        
        if (syncMode == null)
            throw new IllegalArgumentException("log append synchronization mode needs to be specified!");
        
        if (maxQ < 0)
            throw new IllegalArgumentException("max. request queue length must be >= 0!");
        
        if (maxNumRecordsPerBlock <= 0)
            throw new IllegalArgumentException("number of records per block must be > 0!");
        
        if (maxBlockFileSize <= 0)
            throw new IllegalArgumentException("maximum block file size must be > 0!");
        
    }
    
}