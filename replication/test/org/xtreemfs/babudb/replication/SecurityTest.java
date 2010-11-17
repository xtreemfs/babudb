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
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.api.DatabaseManager;
import org.xtreemfs.babudb.api.SnapshotManager;
import org.xtreemfs.foundation.logging.Logging;

import static org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode.NO_ACCESS;

public class SecurityTest {
    
    public final static boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    
    private final static String DB_NAME = "test";
    
    private BabuDB slave;
    private BabuDBConfig conf;
    
    @Before
    public void setUp() throws Exception {   
        Logging.start(Logging.LEVEL_ERROR);
        
        try {
            conf = new BabuDBConfig("config/replication.properties");
            ReplicationConfig replConf = new ReplicationConfig(
                    "config/replication.properties", conf);
            
            Process p;
            if (WIN) {
                p = Runtime.getRuntime().exec("cmd /c rd /s /q \"" + 
                        conf.getBaseDir() + "\"");
            } else 
                p = Runtime.getRuntime().exec("rm -rf " + 
                        conf.getBaseDir());
            p.waitFor();
            
            if (WIN) {
                p = Runtime.getRuntime().exec("cmd /c rd /s /q \"" + 
                        conf.getDbLogDir() + "\"");
            } else 
                p = Runtime.getRuntime().exec("rm -rf " + 
                        conf.getDbLogDir());
            p.waitFor();
            
            if (WIN) {
                p = Runtime.getRuntime().exec("cmd /c rd /s /q \"" + 
                        replConf.getTempDir() + "\"");
            } else 
                p = Runtime.getRuntime().exec("rm -rf " + replConf.getTempDir());
            p.waitFor();
        
            // start the slave
            slave = BabuDBFactory.createBabuDB(conf,new StaticInitialization() {
                
                @Override
                public void initialize(DatabaseManager dbMan, SnapshotManager sMan) {
                    try {
                        dbMan.createDatabase(DB_NAME, 2);
                    } catch (BabuDBException e) {
                        System.out.println("ERROR: "+e.getMessage());
                    }
                }
            });
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