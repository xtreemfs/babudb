/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.xtreemfs.babudb.config.ReplicationConfig;

/**
 * <p>Methods to perform direct file Operations.<br><br>
 * Crash consistency is ensured by a <code>BACKUP_LOCK_FILE</code> file, that is written to the backup directory, 
 * if a backup is written completely and removed, if the new state was loaded successfully.</p>
 * 
 * @author flangner
 * @since 06/11/2009
 */

public class DirectFileIO {
    private static final String BACKUP_LOCK_FILE = ".backupLock";
    
    /**
     * <p>First it removes the BACKUP_LOCK_FILE, than the depreciated backupFiles.</p>
     * 
     * @param configuration
     */
    public static void removeBackupFiles(ReplicationConfig configuration) {
        File backupDir = new File(configuration.getBackupDir());
        if (backupDir.exists()) { 
            File backupLock = new File (backupDir.getPath()+File.separator+BACKUP_LOCK_FILE);
            if (backupLock.exists()) backupLock.delete();
            cleanUpFiles(backupDir);
            backupDir.delete();
        }
    }
    
    /**
     * <p>Replays the backup files, if necessary.</p>
     * 
     * @param configuration
     * @throws IOException 
     */
    public static void replayBackupFiles(ReplicationConfig configuration) throws IOException {
        File backupDir = new File(configuration.getBackupDir());
        if (backupDir.exists()) {
            File backupLock = new File (backupDir.getPath()+File.separator+BACKUP_LOCK_FILE);
            if (backupLock.exists()) {
                File baseDir = new File(configuration.getBaseDir());
                File logDir = new File(configuration.getDbLogDir());
                
                cleanUpFiles(logDir);
                cleanUpFiles(baseDir);
                
                File backupBaseDir = new File(backupDir.getPath()+File.separator+baseDir.getName());
                File backupLogDir = new File(backupDir.getPath()+File.separator+logDir.getName());
                copyDir(backupBaseDir, baseDir);
                copyDir(backupLogDir, logDir);
            }
            
            cleanUpFiles(backupDir);
            backupDir.delete();
        }
    }
    
    /**
     * <p>Makes a backup of the current files and removes them from the working directory.
     * Writes the BACKUP_LOCK_FILE, when finished.
     * <br><br>
     * For slaves only. </p>
     * 
     * @param configuration
     * @throws IOException
     */
    public static void backupFiles(ReplicationConfig configuration) throws IOException {
        File backupDir = new File(configuration.getBackupDir());
        File baseDir = new File(configuration.getBaseDir());
        File logDir = new File(configuration.getDbLogDir());
        
        if (!backupDir.exists())
            backupDir.mkdirs();
        
        File backupLock = new File (backupDir.getPath()+File.separator+BACKUP_LOCK_FILE);
        if (!backupLock.exists()) {
            cleanUpFiles(backupDir);
            File backupBaseDir = new File(backupDir.getPath()+File.separator+baseDir.getName());
            backupBaseDir.mkdir();
            
            File backupLogDir = new File(backupDir.getPath()+File.separator+logDir.getName());
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
     * <p>Copies the directories including files recursively from sourceDir to destDir.</p>
     * 
     * @param sourceDir
     * @param destDir
     * @param exclude - name of files to exclude from copying.
     * @throws IOException
     */
    private static void copyDir(File sourceDir, File destDir) throws IOException {
        File[] files = sourceDir.listFiles();
        File newFile = null;
        
        for (File f : files) {
            if (f.isFile()) {
                newFile = new File(destDir.getPath()+File.separator+f.getName());
                newFile.createNewFile();
                
                copyFile(f, newFile);
            } else if (f.isDirectory()) {
                newFile = new File(destDir.getPath()+File.separator+f.getName());
                newFile.mkdir();
                
                copyDir(f, newFile);
            } else
                assert(false);
        }
    }
    
    /**
     * <p>Copies a single file from source to destination.</p>
     * 
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    private static void copyFile(File sourceFile, File destFile) throws IOException {
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
    private static void cleanUpFiles(File parent) {
        if (parent.exists()) {
            assert (parent.isDirectory());
            
            // delete existing files
            for (File f : parent.listFiles()) {
                if (f.isFile())
                    f.delete();
                else if (f.isDirectory()) {
                    cleanUpFiles(f);
                    if (f.listFiles().length == 0)
                        f.delete();
                } else
                    assert(false);
            }
        }
    }
}
