package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.include.common.config.SlaveConfig;

import static org.xtreemfs.babudb.replication.DirectFileIO.*;

public class DirectFileIOTest {

    private static SlaveConfig conf;
    
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
        conf = new SlaveConfig("config/slave.properties");
        conf.read();
    }
    
    @Before
    public void setUpBefore() throws Exception {       
        
        Process p = Runtime.getRuntime().exec("rm -rf " + conf.getBaseDir());
        assertEquals(0, p.waitFor());
        
        p = Runtime.getRuntime().exec("rm -rf " + conf.getDbLogDir());
        assertEquals(0, p.waitFor());
        
        p = Runtime.getRuntime().exec("rm -rf " + conf.getBackupDir());
        assertEquals(0, p.waitFor());
    }

    @Test
    public void testBackupFiles() throws IOException {
        setupTestdata();
        
        String baseDirName = new File(conf.getBaseDir()).getName();
        String logDirName = new File(conf.getDbLogDir()).getName();
        
        backupFiles(conf);
        
        File testFile = new File(conf.getBackupDir() + baseDirName);
        assertTrue(testFile.exists());
   
        testFile = new File(conf.getBackupDir() + baseDirName + File.separator + baseFile);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getBackupDir() + baseDirName + File.separator + baseDir);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getBackupDir() + baseDirName + File.separator + baseDir + File.separator + baseDirFile);
        assertTrue(testFile.exists());
        
        FileReader r = new FileReader(testFile);
        char[] buf = new char[8];
        r.read(buf);
        assertEquals(baseTestString, String.valueOf(buf).trim());
        
        testFile = new File(conf.getBackupDir() + logDirName);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getBackupDir() + logDirName + File.separator + logFile);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getBackupDir() + logDirName + File.separator + logDir);
        assertTrue(testFile.exists());
        
        testFile = new File(conf.getBackupDir() + logDirName + File.separator + logDir + File.separator + logDirFile);
        assertTrue(testFile.exists());
        
        r = new FileReader(testFile);
        buf = new char[8];
        r.read(buf);
        assertEquals(logTestString, String.valueOf(buf).trim());
        
        File empty = new File (conf.getBaseDir());
        assertTrue(empty.isDirectory());
        assertEquals(0, empty.listFiles().length);
        
        empty = new File (conf.getDbLogDir());
        assertTrue(empty.isDirectory());
        assertEquals(0, empty.listFiles().length);
    }
    
    @Test
    public void testRemoveBackupFiles() throws IOException {
        setupTestdata();
        backupFiles(conf);
        removeBackupFiles(conf);
        
        File backup = new File(conf.getBackupDir());
        assertFalse(backup.exists());
    }

    @Test
    public void testReplayBackupFiles() {
        // TODO fail("Not yet implemented");
    }
    
    private void setupTestdata() throws IOException {
        
        // insert base test data
        File base = new File(conf.getBaseDir());
        assertTrue(base.mkdirs());
        
        File bf = new File(conf.getBaseDir() + baseFile);
        assertTrue(bf.createNewFile());
        
        File bd = new File(conf.getBaseDir() + baseDir);
        assertTrue(bd.mkdir());
        
        File bdf = new File(conf.getBaseDir() + baseDir + File.separator + baseDirFile);
        assertTrue(bdf.createNewFile());
        
        FileWriter w = new FileWriter(bdf);
        w.write(baseTestString);
        w.close();
        
        // insert log test data
        File log = new File(conf.getDbLogDir());
        log.mkdirs();
        
        File lf = new File(conf.getDbLogDir() + logFile);
        assertTrue(lf.createNewFile());
        
        File ld = new File(conf.getDbLogDir() + logDir);
        assertTrue(ld.mkdir());
        
        File ldf = new File(conf.getDbLogDir() + logDir + File.separator + logDirFile);
        assertTrue(ldf.createNewFile());
        
        w = new FileWriter(ldf);
        w.write(logTestString);
        w.close();
    }
}