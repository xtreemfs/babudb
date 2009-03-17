/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.net.InetSocketAddress;
import java.util.List;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.include.common.logging.Logging;

/**
 * <p>InformationHolder for all configuration parameters used by BabuDB.</p>
 * 
 * @author flangner
 */

public class BabuDBConfiguration {
    /** Log Level. */
    static final int            DEBUG_LEVEL     = Logging.LEVEL_WARN;
    
    /**
     * The database configuration file.
     * Contains the names and number of indices for the databases.
     */
    public static final String  DBCFG_FILE      = "config.db";    
    
    /**
     * Base directory to store database index snapshots in.
     */
    private final String        baseDir;
    
    /**
     * Directory in which the database logs are stored.
     */
    private final String        dbLogDir;
    
    /**
     * SyncMode the synchronization mode to use for the logFile.
     */
    private final SyncMode      syncMode;  
    
    /**
     * Max queue length: if > 0, the queue for each worker is limited to maxQ.
     */
    private final int           maxQueueLength;
    
    /**
     * NumThreads number of worker threads to use.
     */
    private final int           numThreads;
    
    /**
     * MaxLogfileSize a checkpoint is generated ,if maxLogfileSize is exceeded.
     */
    private final long          maxLogfileSize;
    
    /**
     * CheckInterval interval between two checks in seconds, 0 disables auto checkPointing.
     */
    private final int           checkInterval;
 
/** 
 * Replication Configuration:
 */
    
    /**
     * Needed for MasterSlave failOver in case of Replication.
     */
    List<InetSocketAddress>     replication_slaves = null;

    /**
     * Needed for MasterSlave failOver in case of Replication.
     */
    InetSocketAddress           replication_master = null;
    
    /**
     * If set to a value > 0, operations are acknowledges immediately before
     * they are written to the disk log. The disk logger will do batch writes
     * and call fSync... every pseudoSyncWait seconds. This can be used to
     * increase performance and emulate PostgreSQL behavior.
     */
    private final int pseudoSyncWait;
    
    BabuDBConfiguration(String baseDir,String dbLogDir,int maxQ,int numThreads,SyncMode syncMode,long maxLogfileSize,int checkInterval,int pseuoSyncWait){
        this.baseDir = baseDir;
        this.dbLogDir = dbLogDir;
        this.maxQueueLength = maxQ;
        this.numThreads = numThreads;
        this.syncMode = syncMode;     
        this.maxLogfileSize = maxLogfileSize;
        this.checkInterval = checkInterval;
        this.pseudoSyncWait = pseuoSyncWait;
    }

    /**
     * Error message.
     */
    static final String slaveProtection = "You are not allowed to proceed this operation, " +
            "because this DB is running as a slave!";

/*
 * Getters    
 */
    
    /**
     * @return the baseDir
     */
    public String getBaseDir() {
        return baseDir;
    }

    /**
     * @return the dbLogDir
     */
    public String getDbLogDir() {
        return dbLogDir;
    }

    /**
     * @return the syncMode
     */
    SyncMode getSyncMode() {
        return syncMode;
    }

    /**
     * @return the maxQueueLength
     */
    int getMaxQueueLength() {
        return maxQueueLength;
    }

    /**
     * @return the numThreads
     */
    int getNumThreads() {
        return numThreads;
    }

    /**
     * @return the maxLogfileSize
     */
    long getMaxLogfileSize() {
        return maxLogfileSize;
    }

    /**
     * @return the checkInterval
     */
    int getCheckInterval() {
        return checkInterval;
    }

    /**
     * @return the pseudoSyncWait
     */
    int getPseudoSyncWait() {
        return pseudoSyncWait;
    }
}
