/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.transmission.FileIO.*;
import static java.io.File.separator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.TestParameters;
import org.xtreemfs.babudb.replication.transmission.FileIO;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * @author flangner
 */

public class DirectFileIOTest {

    private ReplicationConfig conf;
    private FileIOInterface fileIO;
    
    // define the test data
    private final String baseFile = "base.file";
    private final String baseDir = "baseDir";
    private final String baseDirFile = "baseDir.file";
    private final String baseTestString = "base";
    
    private final String logFile = "log.file";
    private final String logDir = "logDir";
    private final String logDirFile = "logDir.file";
    private final String logTestString = "log";
            
    @Before
    public void setUpBefore() throws Exception {   
        conf = new ReplicationConfig("config/replication_server0.test", TestParameters.conf0);
        FSUtils.delTree(new File(conf.getBabuDBConfig().getBaseDir()));
        FSUtils.delTree(new File(conf.getBabuDBConfig().getDbLogDir()));
        FSUtils.delTree(new File(conf.getTempDir()));
        fileIO = new FileIO(conf);
    }
    
    @After
    public void tearDown() throws Exception {
        FSUtils.delTree(new File(conf.getBabuDBConfig().getBaseDir()));
        FSUtils.delTree(new File(conf.getBabuDBConfig().getDbLogDir()));
        FSUtils.delTree(new File(conf.getTempDir()));
    }

    @Test
    public void testBackupFiles() throws IOException {
        setupTestdata();
        
        fileIO.backupFiles();
        
        File testFile = new File(conf.getTempDir() + BACKUP_BASE_DIR + separator );
        assertTrue(testFile.exists());
   
        testFile = new File(conf.getTempDir() + BACKUP_BASE_DIR + separator + baseFile);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + BACKUP_BASE_DIR + separator + baseDir);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + BACKUP_BASE_DIR + separator + baseDir + separator + baseDirFile);
        assertTrue(testFile.exists());
        
        FileReader r = new FileReader(testFile);
        char[] buf = new char[8];
        r.read(buf);
        r.close();
        assertEquals(baseTestString, String.valueOf(buf).trim());
        
        testFile = new File(conf.getTempDir() + BACKUP_LOG_DIR + separator);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + BACKUP_LOG_DIR + separator + logFile);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + BACKUP_LOG_DIR + separator + logDir);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + BACKUP_LOG_DIR + separator + logDir + separator + logDirFile);
        assertTrue(testFile.exists());
        
        r = new FileReader(testFile);
        buf = new char[8];
        r.read(buf);
        r.close();
        assertEquals(logTestString, String.valueOf(buf).trim());
        
        File empty = new File (conf.getBabuDBConfig().getBaseDir());
        assertTrue(empty.isDirectory());
        assertEquals(0, empty.listFiles().length);
        
        empty = new File (conf.getBabuDBConfig().getDbLogDir());
        assertTrue(empty.isDirectory());
        assertEquals(0, empty.listFiles().length);
    }
    
    @Test
    public void testRemoveBackupFiles() throws IOException, InterruptedException {
        setupTestdata();
        fileIO.backupFiles();
        fileIO.removeBackupFiles();
        
        File backup = new File(conf.getTempDir());
        
        if (backup.exists()) Thread.sleep(1000);
        assertFalse(backup.exists());
    }

    @Test
    public void testReplayBackupFiles() throws IOException {
        
        // make a backup
        setupTestdata();
        fileIO.backupFiles();
        
        // replay it
        fileIO.replayBackupFiles();
        
        // check, if everything went fine
        File base = new File(conf.getBabuDBConfig().getBaseDir());
        assertTrue(base.exists());
        
        File bf = new File(conf.getBabuDBConfig().getBaseDir() + baseFile);
        assertTrue(bf.exists());
        
        File bd = new File(conf.getBabuDBConfig().getBaseDir() + baseDir);
        assertTrue(bd.exists());
        
        File bdf = new File(conf.getBabuDBConfig().getBaseDir() + baseDir + 
                separator + baseDirFile);
        assertTrue(bdf.exists());
        
        BufferedReader r = new BufferedReader(new FileReader(bdf));
        assertEquals(baseTestString,r.readLine());
        r.close();
        
        // insert log test data
        File log = new File(conf.getBabuDBConfig().getDbLogDir());
        assertTrue(log.exists());
        
        File lf = new File(conf.getBabuDBConfig().getDbLogDir() + logFile);
        assertTrue(lf.exists());
        
        File ld = new File(conf.getBabuDBConfig().getDbLogDir() + logDir);
        assertTrue(ld.exists());
        
        File ldf = new File(conf.getBabuDBConfig().getDbLogDir() + logDir + 
                separator + logDirFile);
        assertTrue(ldf.exists());
        
        r = new BufferedReader(new FileReader(ldf));
        assertEquals(logTestString,r.readLine());
        r.close();
    }
    
    private void setupTestdata() throws IOException {
        
        // insert base test data
        File base = new File(conf.getBabuDBConfig().getBaseDir());
        assertTrue(base.mkdirs());
        base = null;
        
        File bf = new File(conf.getBabuDBConfig().getBaseDir() + baseFile);
        assertTrue(bf.createNewFile());
        bf = null;
        
        File bd = new File(conf.getBabuDBConfig().getBaseDir() + baseDir);
        assertTrue(bd.mkdir());
        bd = null;
        
        File bdf = new File(conf.getBabuDBConfig().getBaseDir() + baseDir + 
                separator + baseDirFile);
        assertTrue(bdf.createNewFile());
        
        FileWriter w = new FileWriter(bdf);
        w.write(baseTestString);
        w.flush();
        w.close();
        w = null;
        bdf = null;
        
        // insert log test data
        File log = new File(conf.getBabuDBConfig().getDbLogDir());
        log.mkdirs();
        log = null;
        
        File lf = new File(conf.getBabuDBConfig().getDbLogDir() + logFile);
        assertTrue(lf.createNewFile());
        lf = null;
        
        File ld = new File(conf.getBabuDBConfig().getDbLogDir() + logDir);
        assertTrue(ld.mkdir());
        ld = null;
        
        File ldf = new File(conf.getBabuDBConfig().getDbLogDir() + logDir + 
                separator + logDirFile);
        assertTrue(ldf.createNewFile());
        
        w = new FileWriter(ldf);
        w.write(logTestString);
        w.flush();
        w.close();
        w = null;
        ldf = null;
    }
}