/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.include.common.config.SlaveConfig;
import org.xtreemfs.include.common.logging.Logging;

public class SecurityTest {
    private BabuDB slave;
    private SlaveConfig conf;
    
    
    @Before
    public void setUp() throws Exception {   
        Logging.start(Logging.LEVEL_DEBUG);
        
        conf = new SlaveConfig("config/slave.properties");
        conf.read();
        
        Process p = Runtime.getRuntime().exec("rm -rf "+conf.getBaseDir());
        p.waitFor();
        
        p = Runtime.getRuntime().exec("rm -rf "+conf.getDbLogDir());
        p.waitFor();
        
        p = Runtime.getRuntime().exec("rm -rf "+conf.getBackupDir());
        p.waitFor();
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();
        slaves.add(conf.getSlaves().get(0));
        
        // start the slave
        try {
        slave = BabuDB.getSlaveBabuDB(conf);
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
            slave.asyncInsert(null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.asyncLookup(null, 0, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.asyncPrefixLookup(null, 0, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.asyncUserDefinedLookup(null, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.checkpoint();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.copyDatabase(null, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createDatabase(null, 0);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createDatabase(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createInsertGroup(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createSnapshot(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.deleteDatabase(null, true);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.directInsert(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.directLookup(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.directPrefixLookup(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncInsert(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncPrefixLookup(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncSingleInsert(null, 0, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncUserDefinedLookup(null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.writeSnapshot(null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_INFO, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
    }
}
