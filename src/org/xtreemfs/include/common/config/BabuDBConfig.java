/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Reading configurations from the babuDB-config-file.
 * 
 * @since 06/05/2009
 * @author flangner
 *
 */

public class BabuDBConfig extends Config {
    
    /** Log Level. */
    protected int        debugLevel;
    protected String     debugCategory;
    
    /**
     * The database configuration file.
     * Contains the names and number of indices for the databases.
     */
    protected String    dbCfgFile;    
    
    /**
     * Base directory to store database index snapshots in.
     */
    protected String    baseDir;
    
    /**
     * Directory in which the database logs are stored.
     */
    protected String    dbLogDir;
    
    /**
     * SyncMode the synchronization mode to use for the logFile.
     */
    protected SyncMode  syncMode;
    
    /**
     * Max queue length: if > 0, the queue for each worker is limited to maxQ.
     * The replication remote-request-queue will also be limited to maxQ.
     */
    protected int       maxQueueLength;
    
    /**
     * NumThreads number of worker threads to use.
     */
    protected int       numThreads;
    
    /**
     * MaxLogfileSize a checkpoint is generated ,if maxLogfileSize is exceeded.
     */
    protected long      maxLogfileSize;
    
    /**
     * CheckInterval interval between two checks in seconds, 0 disables auto checkPointing.
     */
    protected int       checkInterval;
    
    /**
     * If set to a value > 0, operations are acknowledges immediately before
     * they are written to the disk log. The disk logger will do batch writes
     * and call fSync... every pseudoSyncWait seconds. This can be used to
     * increase performance and emulate PostgreSQL behavior.
     */
    protected int       pseudoSyncWait;
    
    /**
     * Indicates if compression is enabled or not.
     */
    protected boolean	compression;
    
    public BabuDBConfig(String baseDir, String dbLogDir, int numThreads, 
            long maxLogFileSize, int checkInterval,SyncMode syncMode,  
            int pseudoSyncWait, int maxQ, boolean compression) {
        
        super();
        this.debugLevel = Logging.LEVEL_WARN;
        this.debugCategory = "all";
        this.baseDir = (baseDir.endsWith(File.separator)) ? baseDir : baseDir+File.separator;
        this.dbCfgFile = "config.db";
        this.dbLogDir = (dbLogDir.endsWith(File.separator)) ? dbLogDir : dbLogDir+File.separator;
        this.syncMode = syncMode;
        this.maxQueueLength = maxQ;
        this.numThreads = numThreads;
        this.checkInterval = checkInterval;
        this.pseudoSyncWait = pseudoSyncWait;
        this.maxLogfileSize = maxLogFileSize;
        this.compression = compression;
    }
    
    public BabuDBConfig() {
        super();
    }
    
    public BabuDBConfig(Properties prop) throws IOException {
        super(prop);
        read();
    }
    
    public BabuDBConfig(String filename) throws IOException {
        super(filename);
        read();
    }
    
    public void read() throws IOException {
        
        this.debugLevel = this.readOptionalInt("debug.level", Logging.LEVEL_WARN);
        
        this.debugCategory = this.readOptionalString("debug.category", "all");
        
        this.dbCfgFile = this.readOptionalString("db.cfgFile", "config.db");
        
        String baseDir = this.readRequiredString("db.baseDir");
        this.baseDir = (baseDir.endsWith(File.separator)) ? baseDir : baseDir+File.separator;
        
        String dbLogDir = this.readRequiredString("db.logDir");
        this.dbLogDir = (dbLogDir.endsWith(File.separator)) ? dbLogDir : dbLogDir+File.separator;

        this.syncMode = SyncMode.valueOf(this.readRequiredString("db.sync"));
        
        this.numThreads = this.readOptionalInt("worker.numThreads", 1);
        
        this.maxQueueLength = this.readOptionalInt("worker.maxQueueLength",0);
        
        this.maxLogfileSize = this.readOptionalInt("db.maxLogfileSize", 1);
        
        this.checkInterval = this.readOptionalInt("db.checkInterval", 0);
        
        this.pseudoSyncWait = this.readOptionalInt("db.pseudoSyncWait", 0);
        
        this.compression = this.readOptionalBoolean("db.compression", false);
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
}