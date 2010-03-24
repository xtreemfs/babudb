/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.include.common.logging.Logging;

import static org.xtreemfs.babudb.BabuDBException.ErrorCode.NO_ACCESS;

public class SecurityTest {
    
    public final static boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    
    private final static String DB_NAME = "test";
    
    private BabuDB slave;
    private ReplicationConfig conf;
    
    @Before
    public void setUp() throws Exception {   
        Logging.start(Logging.LEVEL_DEBUG);
        
        try {
            conf = new ReplicationConfig("config/replication.properties");
            
            Process p;
            if (WIN) {
                p = Runtime.getRuntime().exec("cmd /c rd /s /q \"" + conf.getBaseDir() + "\"");
            } else 
                p = Runtime.getRuntime().exec("rm -rf " + conf.getBaseDir());
            assertEquals(0, p.waitFor());
            
            if (WIN) {
                p = Runtime.getRuntime().exec("cmd /c rd /s /q \"" + conf.getDbLogDir() + "\"");
            } else 
                p = Runtime.getRuntime().exec("rm -rf " + conf.getDbLogDir());
            assertEquals(0, p.waitFor());
            
            if (WIN) {
                p = Runtime.getRuntime().exec("cmd /c rd /s /q \"" + conf.getBackupDir() + "\"");
            } else 
                p = Runtime.getRuntime().exec("rm -rf " + conf.getBackupDir());
            assertEquals(0, p.waitFor());
        
            // start the slave
            slave = BabuDBFactory.createReplicatedBabuDB(conf,null);
            ((DatabaseManagerImpl) slave.getDatabaseManager()).proceedCreate(DB_NAME, 2, null);
        } catch (Exception e){
        	System.out.println("ERROR: "+e.getMessage());
        }
    }
    
    @After
    public void tearDown() throws Exception {
        Logging.logMessage(Logging.LEVEL_INFO, slave, "shutting down databases...");
        slave.shutdown();
    }
    
    /**
     * Check the safety of the BabuDB interface on slave-mode. 
     */
    @Test
    public void slaveDBAccessSecurityTest() throws Exception {   
        
        try{
            slave.getCheckpointer().checkpoint();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().copyDatabase(null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().createDatabase(null, 0);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().createDatabase(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).createInsertGroup();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().deleteDatabase(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
                
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).insert(null,null).get();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).prefixLookup(0, null,null).get();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).singleInsert(0, null, null,null).get();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).userDefinedLookup(null,null).get();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(NO_ACCESS,be.getErrorCode());
        }
    }
}