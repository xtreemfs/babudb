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

import org.xtreemfs.include.common.config.SlaveConfig;

/**
 * Methods to perform direct file Operations.
 * 
 * @author flangner
 * @since 06/11/2009
 */

public class DirectFileIO {
    
    /**
     * Removes depreciated backupFiles. 
     * 
     * @param configuration
     */
    public static void removeBackupFiles(SlaveConfig configuration) {
        File backupDir = new File(configuration.getBackupDir());
        assert(backupDir.exists());
        
        cleanUpFiles(backupDir);
        backupDir.delete();
    }
    
    public static void replayBackupFiles(SlaveConfig configuration) {
        // TODO
    }
    
    /**
     * Makes a backup of the current files and removes them from the working directory.
     * For slaves only.
     * 
     * @param configuration
     * @throws IOException
     */
    public static void backupFiles(SlaveConfig configuration) throws IOException {
        File backupDir = new File(configuration.getBackupDir());
        File baseDir = new File(configuration.getBaseDir());
        File logDir = new File(configuration.getDbLogDir());
        if (!backupDir.exists()) {
            backupDir.mkdirs();
            
            File backupBaseDir = new File(backupDir.getPath()+File.separator+baseDir.getName());
            backupBaseDir.mkdir();
            
            File backupLogDir = new File(backupDir.getPath()+File.separator+logDir.getName());
            if (!backupBaseDir.getPath().equals(backupLogDir.getPath())) backupLogDir.mkdir();
            
            moveDir(baseDir, backupBaseDir);
            moveDir(logDir, backupLogDir);
        } else {       
            cleanUpFiles(baseDir);
            cleanUpFiles(logDir);
        }
    }
    
    private static void moveDir(File sourceDir, File destDir) throws IOException {
        File[] files = sourceDir.listFiles();
        File newFile = null;
        
        for (File f : files) {
            if (f.isFile()) {
                newFile = new File(destDir.getPath()+File.separator+f.getName());
                newFile.createNewFile();
                
                moveFile(f, newFile);
            } else if (f.isDirectory()) {
                newFile = new File(destDir.getPath()+File.separator+f.getName());
                newFile.mkdir();
                
                moveDir(f, newFile);
                f.delete();
            } else 
                assert(false);
        }
    }
    
    /**
     * Moves a single file from source to destination.
     * 
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    private static void moveFile(File sourceFile, File destFile) throws IOException {
        assert (sourceFile.isFile());
        assert (destFile.isFile());
        
        FileChannel source = null;
        FileChannel destination = null;
        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
            sourceFile.delete();
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
        assert (parent.isDirectory());
        
        // delete existing files
        for (File f : parent.listFiles()) {
            if (f.isFile())
                f.delete();
            else if (f.isDirectory()) {
                cleanUpFiles(f);
                f.delete();
            } else {
                assert(false);
            }
        }
    }
}
