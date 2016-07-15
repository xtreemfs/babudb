/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSMDatabase.DBFileMetaData;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.util.FSUtils;

import static java.io.File.*;

/**
 * <p>
 * Methods to perform direct file Operations.<br><br>
 * Crash consistency is ensured by a <code>BACKUP_LOCK_FILE</code> file, 
 * that is written to the backup directory, if a backup is written completely 
 * and removed, if the new state was loaded successfully.
 * </p>
 * 
 * @author flangner
 * @since 06/11/2009
 */

public class FileIO implements FileIOInterface {
    
    /** name for the base directory within the backup folder */
    static final String BACKUP_BASE_DIR = "base";
    
    /** name for the log directory within the backup folder */
    static final String BACKUP_LOG_DIR = "log";
    
    /** name for the backup lock-file */
    static final String BACKUP_LOCK_FILE = ".backupLock";
    
    /** the config file with path definitions */
    private final ReplicationConfig configuration;
    
    /**
     * 
     * @param config
     */
    public FileIO(ReplicationConfig config) {
        this.configuration = config;
    }

/*
 * Overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#
     * getConfigFileMetaData()
     */
    @Override
    public DBFileMetaData getConfigFileMetaData() {        
        String path = configuration.getBabuDBConfig().getBaseDir() + 
                      configuration.getBabuDBConfig().getDbCfgFile();
        
        long length = new File(path).length();
        
        return new DBFileMetaData(path, length);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#
     * getFile(java.lang.String)
     */
    @Override
    public File getFile(String fileName) throws IOException {
        
        File chnk = new File(fileName);
        String fName = chnk.getName();
        String pName = chnk.getParentFile().getName();
        File result;
        String baseDir = configuration.getBabuDBConfig().getBaseDir();
        
        if (LSMDatabase.isSnapshotFilename(pName)) {
            // create the db-name directory, if necessary
            new File(baseDir + pName + separatorChar).mkdirs();
            // create the file if necessary
            result = new File(baseDir + 
                         chnk.getParentFile().getParentFile().getName() +
                         separatorChar +  pName + separator + fName);
            result.getParentFile().mkdirs();
            result.createNewFile();
        } else {
            // create the file if necessary
            result = new File(baseDir + configuration.getBabuDBConfig().getDbCfgFile());
            result.getParentFile().mkdirs();
            result.createNewFile();
        } 
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#
     * getLogEntryIterator(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public DiskLogIterator getLogEntryIterator(LSN from) 
            throws LogEntryException, IOException {        
        return new DiskLogIterator(getLogFiles(), from);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#
     * removeBackupFiles()
     */
    @Override
    public void removeBackupFiles() {
        File backupDir = new File(configuration.getTempDir());
        if (backupDir.exists()) { 
            File backupLock = new File (backupDir.getPath() + separator +
                    BACKUP_LOCK_FILE);
            if (backupLock.exists()) backupLock.delete();
            FSUtils.delTree(backupDir);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#
     * replayBackupFiles()
     */
    @Override
    public void replayBackupFiles() throws IOException {
        File backupDir = new File(configuration.getTempDir());
        if (backupDir.exists()) {
            File backupLock = new File (backupDir.getPath() + separator +
                    BACKUP_LOCK_FILE);
            if (backupLock.exists()) {
                File baseDir = new File(configuration.getBabuDBConfig().getBaseDir());
                File logDir = new File(configuration.getBabuDBConfig().getDbLogDir());
                
                cleanUpFiles(logDir);
                cleanUpFiles(baseDir);
                assert (baseDir.listFiles().length == 0);
                assert (logDir.listFiles().length == 0);
                
                File backupBaseDir = new File(backupDir.getPath() + separator + BACKUP_BASE_DIR + 
                        separator);
                File backupLogDir = new File(backupDir.getPath() + separator + BACKUP_LOG_DIR +
                        separator);
                
                FSUtils.copyTree(backupBaseDir, baseDir);
                FSUtils.copyTree(backupLogDir, logDir);
            }
            
            cleanUpFiles(backupDir);
            assert (backupDir.listFiles().length == 0);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#
     * backupFiles()
     */
    @Override
    public void backupFiles() throws IOException {
        File backupDir = new File(configuration.getTempDir());
        File baseDir = new File(configuration.getBabuDBConfig().getBaseDir());
        File logDir = new File(configuration.getBabuDBConfig().getDbLogDir());
        
        if (!backupDir.exists()) {
            backupDir.mkdirs();
        }
        
        File backupLock = new File (backupDir.getPath() + separator + BACKUP_LOCK_FILE);
        if (!backupLock.exists()) {
            cleanUpFiles(backupDir);
            assert (backupDir.listFiles().length == 0);
            
            File backupBaseDir = new File(backupDir.getPath() + separator + BACKUP_BASE_DIR + 
                    separator);
            backupBaseDir.mkdir();
            
            File backupLogDir = new File(backupDir.getPath() + separator + BACKUP_LOG_DIR +
                    separator);
            backupLogDir.mkdir();
            
            FSUtils.copyTree(baseDir, backupBaseDir);
            FSUtils.copyTree(logDir, backupLogDir);
            
            backupLock.createNewFile();
        }
        cleanUpFiles(baseDir);
        cleanUpFiles(logDir);
        
        assert (baseDir.listFiles().length == 0);
        assert (logDir.listFiles().length == 0);
    }
    
/*
 * private methods
 */
    
    /**
     * <p>Clean Up operation to avoid file ambiguity.</p> 
     * 
     * @param parent - the path to cleanUp.
     */
    private void cleanUpFiles(File parent) {
        if (parent.exists()) {
            assert (parent.isDirectory());
            
            // delete existing files
            for (File f : parent.listFiles()) {
                if (f.isFile()) {
                    f.delete();
                } else if (f.isDirectory()) {
                    FSUtils.delTree(f);
                } else {
                    assert(false);
                }
            }
        }
    }
    
    /**
     * @return an array of log-files found in the database log-directory.
     */
    private File[] getLogFiles() {
        File f = new File(configuration.getBabuDBConfig().getDbLogDir());
        File[] result = f.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".dbl");
            }
        });
        
        return result;
    }
}
