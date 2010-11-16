/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.log.DiskLogIterator;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.foundation.util.FSUtils;

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
    
    /** name for the backup lock-file */
    private static final String BACKUP_LOCK_FILE = ".backupLock";
    
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
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#getConfigFileLength()
     */
    @Override
    public DBFileMetaData getConfigFileMetaData() {        
        String path = this.configuration.getBabuDBConfig().getBaseDir() + 
                      this.configuration.getBabuDBConfig().getDbCfgFile();
        
        long length = new File(path).length();
        
        return new DBFileMetaData(path, length, 
                this.configuration.getChunkSize());
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#getFile(org.xtreemfs.babudb.interfaces.Chunk)
     */
    @Override
    public File getFile(Chunk chunk) throws IOException {
        
        File chnk = new File(chunk.getFileName());
        String fName = chnk.getName();
        String pName = chnk.getParentFile().getName();
        File result;
        String baseDir = this.configuration.getBabuDBConfig().getBaseDir();
        
        if (LSMDatabase.isSnapshotFilename(pName)) {
            // create the db-name directory, if necessary
            new File(baseDir + pName + File.separatorChar).mkdirs();
            // create the file if necessary
            result = new File(baseDir + 
                         chnk.getParentFile().getParentFile().getName() +
                         File.separatorChar +  pName +
                         File.separator + fName);
            result.getParentFile().mkdirs();
            result.createNewFile();
        } else {
            // create the file if necessary
            result = new File(baseDir + 
                    this.configuration.getBabuDBConfig().getDbCfgFile());
            result.getParentFile().mkdirs();
            result.createNewFile();
        } 
        return result;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#getLogEntryIterator(org.xtreemfs.babudb.lsmdb.LSN)
     */
    @Override
    public DiskLogIterator getLogEntryIterator(LSN from) 
            throws LogEntryException, IOException {        
        return new DiskLogIterator(getLogFiles(), from);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#removeBackupFiles()
     */
    @Override
    public void removeBackupFiles() {
        File backupDir = new File(this.configuration.getTempDir());
        if (backupDir.exists()) { 
            File backupLock = new File (backupDir.getPath() + File.separator +
                    BACKUP_LOCK_FILE);
            if (backupLock.exists()) backupLock.delete();
            FSUtils.delTree(backupDir);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#replayBackupFiles()
     */
    @Override
    public void replayBackupFiles() throws IOException {
        File backupDir = new File(this.configuration.getTempDir());
        if (backupDir.exists()) {
            File backupLock = new File (backupDir.getPath() + File.separator +
                    BACKUP_LOCK_FILE);
            if (backupLock.exists()) {
                File baseDir = new File(
                        this.configuration.getBabuDBConfig().getBaseDir());
                File logDir = new File(
                        this.configuration.getBabuDBConfig().getDbLogDir());
                
                cleanUpFiles(logDir);
                cleanUpFiles(baseDir);
                
                File backupBaseDir = new File(backupDir.getPath() + 
                        File.separator + baseDir.getName());
                File backupLogDir = new File(backupDir.getPath() + 
                        File.separator + logDir.getName());
                copyDir(backupBaseDir, baseDir);
                copyDir(backupLogDir, logDir);
            }
            
            FSUtils.delTree(backupDir);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.FileIOInterface#backupFiles()
     */
    @Override
    public void backupFiles() throws IOException {
        File backupDir = new File(this.configuration.getTempDir());
        File baseDir = new File(
                this.configuration.getBabuDBConfig().getBaseDir());
        File logDir = new File(
                this.configuration.getBabuDBConfig().getDbLogDir());
        
        if (!backupDir.exists())
            backupDir.mkdirs();
        
        File backupLock = new File (backupDir.getPath() + File.separator + 
                BACKUP_LOCK_FILE);
        if (!backupLock.exists()) {
            cleanUpFiles(backupDir);
            File backupBaseDir = new File(backupDir.getPath() + File.separator +
                    baseDir.getName());
            backupBaseDir.mkdir();
            
            File backupLogDir = new File(backupDir.getPath() + File.separator +
                    logDir.getName());
            backupLogDir.mkdir();
            
            copyDir(baseDir, backupBaseDir);
            copyDir(logDir, backupLogDir);
            
            backupLock.createNewFile();
        }      
        cleanUpFiles(baseDir);
        cleanUpFiles(logDir);
    }
    
/*
 * private methods
 */
    
    /**
     * <p>
     * Copies the directories including files recursively from sourceDir to 
     * destDir.
     * </p>
     * 
     * @param sourceDir
     * @param destDir
     * @param exclude - name of files to exclude from copying.
     * @throws IOException
     */
    private void copyDir(File sourceDir, File destDir) throws IOException {
        File[] files = sourceDir.listFiles();
        File newFile = null;
        
        for (File f : files) {
            if (f.isFile()) {
                newFile = new File(destDir.getPath() + File.separator + 
                        f.getName());
                newFile.createNewFile();
                
                copyFile(f, newFile);
            } else if (f.isDirectory()) {
                newFile = new File(destDir.getPath() + File.separator + 
                        f.getName());
                newFile.mkdir();
                
                copyDir(f, newFile);
            } else {
                assert(false);
            }
        }
    }
    
    /**
     * <p>Copies a single file from source to destination.</p>
     * 
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    private void copyFile(File sourceFile, File destFile) throws IOException {
        assert (sourceFile.isFile());
        assert (destFile.isFile());
        
        FileChannel source = null;
        FileChannel destination = null;
        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            if(source != null) source.close();
            if(destination != null) destination.close();
        }
    }
    
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
        File f = new File(this.configuration.getBabuDBConfig().getDbLogDir());
        File[] result = f.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".dbl");
            }
        });
        
        return result;
    }
}
