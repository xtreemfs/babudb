/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.replication.transmission.FileIO;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * 
 * @author flangner
 *
 */

public class DirectFileIOTest {

    private static ReplicationConfig conf;
    private static FileIOInterface fileIO;
    
    // define the test data
    String baseFile = "base.file";
    String baseDir = "baseDir";
    String baseDirFile = "baseDir.file";
    String baseTestString = "base";
    
    String logFile = "log.file";
    String logDir = "logDir";
    String logDirFile = "logDir.file";
    String logTestString = "log";
        
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf = new ReplicationConfig("config/replication.properties", 
                new BabuDBConfig("config/replication.properties"));
        fileIO = new FileIO(conf);
    }
    
    @Before
    public void setUpBefore() throws Exception {   
        FSUtils.delTree(new File(conf.getBabuDBConfig().getBaseDir()));
        FSUtils.delTree(new File(conf.getBabuDBConfig().getDbLogDir()));
        FSUtils.delTree(new File(conf.getTempDir()));
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
        
        String baseDirName = new File(conf.getBabuDBConfig().getBaseDir()).getName();
        String logDirName = new File(conf.getBabuDBConfig().getDbLogDir()).getName();
        
        fileIO.backupFiles();
        
        File testFile = new File(conf.getTempDir() + baseDirName);
        assertTrue(testFile.exists());
   
        testFile = new File(conf.getTempDir() + baseDirName + File.separator + baseFile);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + baseDirName + File.separator + baseDir);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + baseDirName + File.separator + baseDir + File.separator + baseDirFile);
        assertTrue(testFile.exists());
        
        FileReader r = new FileReader(testFile);
        char[] buf = new char[8];
        r.read(buf);
        assertEquals(baseTestString, String.valueOf(buf).trim());
        
        testFile = new File(conf.getTempDir() + logDirName);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + logDirName + File.separator + logFile);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + logDirName + File.separator + logDir);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getTempDir() + logDirName + File.separator + logDir + File.separator + logDirFile);
        assertTrue(testFile.exists());
        
        r = new FileReader(testFile);
        buf = new char[8];
        r.read(buf);
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
                File.separator + baseDirFile);
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
                File.separator + logDirFile);
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
                File.separator + baseDirFile);
        assertTrue(bdf.createNewFile());
        
        FileWriter w = new FileWriter(bdf);
        w.write(baseTestString);
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
                File.separator + logDirFile);
        assertTrue(ldf.createNewFile());
        
        w = new FileWriter(ldf);
        w.write(logTestString);
        w.close();
        w = null;
        ldf = null;
    }
}