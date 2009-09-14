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
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.lsmdb.DatabaseManagerImpl;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;

public class SecurityTest {

    private final static String DB_NAME = "test";
    
    private BabuDB slave;
    private ReplicationConfig conf;
    
    @Before
    public void setUp() throws Exception {   
        Logging.start(Logging.LEVEL_DEBUG);
        
        conf = new ReplicationConfig("config/replication.properties");
        conf.read();
        
        Process p = Runtime.getRuntime().exec("rm -rf "+conf.getBaseDir());
        p.waitFor();
        
        p = Runtime.getRuntime().exec("rm -rf "+conf.getDbLogDir());
        p.waitFor();
        
        p = Runtime.getRuntime().exec("rm -rf "+conf.getBackupDir());
        p.waitFor();
                
        // start the slave
        try {
            slave = BabuDBFactory.createBabuDB(conf);
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
            slave.getDatabaseManager().getDatabase(DB_NAME).asyncInsert(null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).asyncLookup(0, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).asyncPrefixLookup(0, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).asyncUserDefinedLookup(null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getCheckpointer().checkpoint();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().copyDatabase(null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().createDatabase(null, 0);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().createDatabase(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).createInsertGroup();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().deleteDatabase(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).directInsert(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).directLookup(0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).directPrefixLookup(0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).syncInsert(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).syncPrefixLookup(0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).syncSingleInsert(0, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.getDatabaseManager().getDatabase(DB_NAME).syncUserDefinedLookup(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
    }
}