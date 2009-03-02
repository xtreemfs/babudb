/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.TestConfiguration.*;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.include.common.logging.Logging;

public class SecurityTest {
    private static BabuDB slave;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {   
        Logging.start(Logging.LEVEL_WARN);
        
        Process p = Runtime.getRuntime().exec("rm -rf "+slave1_baseDir);
        p.waitFor();
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();
        slaves.add(new InetSocketAddress("localhost",PORT));
        
        // start the slave
        slave = BabuDBFactory.getSlaveBabuDB(slave1_baseDir,slave1_baseDir,1,0,0,SyncMode.SYNC_WRITE,
                0,0,new InetSocketAddress("localhost",PORT),slaves,0,null,0);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Logging.logMessage(Logging.LEVEL_TRACE, slave, "shutting down databases...");
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
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.asyncLookup(null, 0, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.asyncPrefixLookup(null, 0, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.asyncUserDefinedLookup(null, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.checkpoint();
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.copyDatabase(null, null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createDatabase(null, 0);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createDatabase(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createInsertGroup(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.createSnapshot(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.deleteDatabase(null, true);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.directInsert(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.directLookup(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.directPrefixLookup(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncInsert(null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncPrefixLookup(null, 0, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncSingleInsert(null, 0, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.syncUserDefinedLookup(null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
        
        try{
            slave.writeSnapshot(null, null, null);
        }catch (BabuDBException be){
            Logging.logMessage(Logging.LEVEL_TRACE, slave, be.getMessage());
            assertEquals(ErrorCode.REPLICATION_FAILURE,be.getErrorCode());
        }
    }
}
