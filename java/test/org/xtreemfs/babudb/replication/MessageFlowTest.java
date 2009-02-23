/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.Replication.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.json.JSONException;
import org.xtreemfs.include.foundation.json.JSONParser;
import org.xtreemfs.include.foundation.json.JSONString;
import org.xtreemfs.include.foundation.pinky.HTTPUtils;
import org.xtreemfs.include.foundation.pinky.PinkyRequest;
import org.xtreemfs.include.foundation.pinky.PinkyRequestListener;
import org.xtreemfs.include.foundation.pinky.PipelinedPinky;
import org.xtreemfs.include.foundation.pinky.HTTPUtils.DATA_TYPE;
import org.xtreemfs.include.foundation.speedy.MultiSpeedy;
import org.xtreemfs.include.foundation.speedy.SpeedyRequest;
import org.xtreemfs.include.foundation.speedy.SpeedyResponseListener;

public class MessageFlowTest implements PinkyRequestListener,SpeedyResponseListener{        
    public static final int PORT = 34567;
    
    public static final InetSocketAddress master_address = new InetSocketAddress("localhost",MASTER_PORT);
    
    public static final InetSocketAddress slave1_address = new InetSocketAddress("localhost",SLAVE_PORT);
    
    public static final String master_baseDir = "/tmp/lsmdb-test/master/";
    
    public static final String slave1_baseDir = "/tmp/lsmdb-test/slave1/";
    
    private static LSN actual = new LSN(1,0L);
    
    private static MultiSpeedy speedy;
    
    private static PipelinedPinky pinky;

    private static BabuDBImpl master;
    
    private static BabuDBImpl slave;
    
    private SpeedyRequest sRq = null;
    
    private PinkyRequest pRq = null;
    
    private final Object testLock = new Object();  
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {   
        Logging.start(Logging.LEVEL_ERROR);
        
        Process p = Runtime.getRuntime().exec("rm -rf "+master_baseDir);
        p.waitFor();
        
        p = Runtime.getRuntime().exec("rm -rf "+slave1_baseDir);
        p.waitFor();
        
        InetSocketAddress snifferAddress = new InetSocketAddress("localhost",PORT);
        
        // start the communication components (stands between master and slave and verifies their communication)
        pinky = new PipelinedPinky(snifferAddress.getPort(),snifferAddress.getAddress(),null);
        pinky.start();
        pinky.waitForStartup();
        
        speedy = new MultiSpeedy();
        speedy.start();
        speedy.waitForStartup();
        
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();
        slaves.add(snifferAddress);
        
        System.out.println("starting up databases...");
        
        // start the master - SYNC replication mode       
        master = (BabuDBImpl) BabuDBFactory.getMasterBabuDB(master_baseDir,master_baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0,slaves,0,null,slaves.size());
        
        // start the slave
        slave = (BabuDBImpl) BabuDBFactory.getSlaveBabuDB(slave1_baseDir,slave1_baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0,snifferAddress,slaves,0,null);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("shutting down databases...");
        pinky.shutdown();
        speedy.shutdown();
        master.shutdown();
        slave.shutdown();
        pinky.waitForShutdown();
        speedy.waitForShutdown();
    }

    @Test
    public void createDBNORMALTest() throws InterruptedException, JSONException, IllegalStateException, IOException, LogEntryException {        
        pinky.registerListener(this);
        speedy.registerSingleListener(this);
        
        final String testDBName1 = "test";
        final int testIndices = 2;
        
        /*
         * CREATE DB
         */        
        synchronized (testLock) {
            
            // asynchronous method-call for DB CREATE 
            Thread asyncCreate = new Thread(new Runnable() {
            
                @Override
                public void run() {
                    try {
                        master.createDatabase(testDBName1, testIndices);
                    } catch (BabuDBException e) {
                        fail("CREATE could not be replicated: "+e.getMessage());
                    }           
                }
            });
            
            asyncCreate.start();
            
            
         // check the CREATE from master
            while (pRq == null)
                testLock.wait();

            
            List<Object> data = (List<Object>) JSONParser.parseJSON(new JSONString(new String(pRq.getBody())));
            
            assertEquals(Token.CREATE,Token.valueOf(pRq.requestURI));
            assertEquals(testDBName1, (String) data.get(0));
            assertEquals(testIndices, Integer.parseInt((String) data.get(1)));
          
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    pRq.requestURI,null,null,
                    ReusableBuffer.wrap(pRq.getBody()),
                    DATA_TYPE.JSON), slave1_address); 
           
         // check the response from slave
            while (sRq == null)
                testLock.wait();
            
            assertEquals(HTTPUtils.SC_OKAY, sRq.statusCode);
            
            sRq.freeBuffer();
            sRq = null;
         
         // send it back to the master   
            pRq.setResponse(HTTPUtils.SC_OKAY);
            pinky.sendResponse(pRq);
            
            pRq = null;
        }
        
        final String[] testKeys = {"Yagga","Brabbel","Blupp"};
        final String[] testData = {"Blahh","Babu","Xtreem"};
        final int testIndexId = 0;
     
        /*
         * Insert 3 entries into the database
         */       
        insertSomeData(testDBName1,testIndexId,testKeys,testData);
    }
    
    @Test
    public void copyDBNORMALTest () throws JSONException, InterruptedException, IllegalStateException, IOException, LogEntryException {
        pinky.registerListener(this);
        speedy.registerSingleListener(this);
        
        final String testDBName1 = "test"; 
        final String testDBName2 = "test2";
        
        /*
         * COPY DB
         */        
        synchronized (testLock) {
            
            // asynchronous method-call for DB COPY 
            Thread asyncCreate = new Thread(new Runnable() {
            
                @Override
                public void run() {
                    try {
                        master.copyDatabase(testDBName1, testDBName2, null, null);
                    } catch (Exception e) {
                        fail("COPY could not be replicated: "+e.getMessage());
                    }           
                }
            });
            
            asyncCreate.start();
            
            
         // check the COPY from master
            while (pRq == null)
                testLock.wait();

            
            List<Object> data = (List<Object>) JSONParser.parseJSON(new JSONString(new String(pRq.getBody())));
            
            assertEquals(Token.COPY,Token.valueOf(pRq.requestURI));
            assertEquals(testDBName1, (String) data.get(0));
            assertEquals(testDBName2, (String) data.get(1));
          
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    pRq.requestURI,null,null,
                    ReusableBuffer.wrap(pRq.getBody()),
                    DATA_TYPE.JSON), slave1_address); 
           
         // check the response from slave
            while (sRq == null)
                testLock.wait();
            
            assertEquals(HTTPUtils.SC_OKAY, sRq.statusCode);
            
            sRq.freeBuffer();
            sRq = null;
         
         // send it back to the master   
            pRq.setResponse(HTTPUtils.SC_OKAY);
            pinky.sendResponse(pRq);
            
            pRq = null;
        }
        
        final String[] testKeys = {"Yagga","Brabbel","Blupp"};
        final String[] testData = {"Blahh","Babu","Xtreem"};
        final int testIndexId = 0;
        
        /*
         * Insert 3 entries into the database
         */        
        insertSomeData(testDBName2,testIndexId,testKeys,testData);
    }
    
    @Test
    public void deleteDBNORMALTest() throws BabuDBException, IOException, InterruptedException, JSONException, LogEntryException{
        pinky.registerListener(this);
        speedy.registerSingleListener(this);
        
        final String testDBName1 = "test";        
        final boolean testDeleteFlag = true;
        
        /*
         * DELETE DB
         */        
        synchronized (testLock) {
            
            // asynchronous method-call for DB DELETE
            Thread asyncCreate = new Thread(new Runnable() {
            
                @Override
                public void run() {
                    try {
                        master.deleteDatabase(testDBName1, testDeleteFlag);
                    } catch (Exception e) {
                        fail("DELETE could not be replicated: "+e.getMessage());
                    }           
                }
            });
            
            asyncCreate.start();
            
            
         // check the DELETE from master
            while (pRq == null)
                testLock.wait();

            
            List<Object> data = (List<Object>) JSONParser.parseJSON(new JSONString(new String(pRq.getBody())));
            
            assertEquals(Token.DELETE,Token.valueOf(pRq.requestURI));
            assertEquals(testDBName1, (String) data.get(0));
            assertEquals(testDeleteFlag, Boolean.valueOf((String) data.get(1)));
         
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    pRq.requestURI,null,null,
                    ReusableBuffer.wrap(pRq.getBody()),
                    DATA_TYPE.JSON), slave1_address); 
           
         // check the response from slave
            while (sRq == null)
                testLock.wait();
            
            assertEquals(HTTPUtils.SC_OKAY, sRq.statusCode);
            
            sRq.freeBuffer();
            sRq = null;
         
         // send it back to the master   
            pRq.setResponse(HTTPUtils.SC_OKAY);
            pinky.sendResponse(pRq);
            
            pRq = null;
        }
    }
    
    @Test
    public void unavailableSlaveTest() throws InterruptedException, BabuDBException{
        pinky.registerListener(this);
        speedy.registerSingleListener(this);
        
        final String testDBName2 = "test2";
        final String lostKey = "lostKey";
        final String lostData = "lostValue";
        final int testIndexId = 0;
        
        List<InetSocketAddress> reset = master.replicationFacade.getSlaves();
        
        final List<InetSocketAddress> testDummySlave = new LinkedList<InetSocketAddress>();
        testDummySlave.add(slave1_address);
        testDummySlave.add(new InetSocketAddress("unknownHost",12345));
        
        master.replicationFacade.setSlaves(testDummySlave);
        
        // apply the replication mode (SYNC)
        master.replicationFacade.setSyncModus(testDummySlave.size());
        
        /*
         * INSERT the lost entry
         */
        
        try {
            master.syncSingleInsert(testDBName2, testIndexId, lostKey.getBytes(), lostData.getBytes());    
            fail("REPLICA should fail!");
        } catch (BabuDBException e) {}    
        
        Thread.sleep(500);
      
        master.replicationFacade.setSlaves(reset);
        actual = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);
    }
    
    @Test
    public void replicaRQTest () throws InterruptedException, LogEntryException, IllegalStateException, IOException {
        pinky.registerListener(this);
        speedy.registerSingleListener(this);
        
        Checksum checksum = new CRC32(); 
        
        final String testDBName2 = "test2";
        final String lostKey = "lostKey";
        final String lostData = "lostValue";
        final int testIndexId = 0;
        
        /*
         * INSERT the lost entry
         */
        
        synchronized (testLock) {
            
            // asynchronous method-call for DB insert 
            Thread asyncInsert = new Thread(new Runnable() {
            
                @Override
                public void run() {                    
                    try {
                        master.syncSingleInsert(testDBName2, testIndexId, lostKey.getBytes(), lostData.getBytes());                      
                    } catch (BabuDBException e) {
                        fail("REPLICA should be send properly!");
                    }                     
                }
            });
            
            asyncInsert.start();
            
            
         // check the REPLICA from master
            while (pRq == null)
                testLock.wait();

            LogEntry logEntry = LogEntry.deserialize(ReusableBuffer.wrap(pRq.getBody()), checksum);
            checksum.reset();
            InsertRecordGroup irec = InsertRecordGroup.deserialize(logEntry.getPayload());                
            LSN lsn = logEntry.getLSN();                
            actual = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);
            
            assertEquals(Token.REPLICA,Token.valueOf(pRq.requestURI));
            assertEquals(actual, lsn);
            assertEquals(testIndexId,irec.getInserts().get(0).getIndexId());            
            assertTrue(lostKey.equals(new String(irec.getInserts().get(0).getKey())));            
            assertTrue(lostData.equals(new String(irec.getInserts().get(0).getValue())));

          // --> DO NOT SEND IT TO THE SLAVE! IT IS LOST.
            
            pRq.setResponse(HTTPUtils.SC_SERV_UNAVAIL);
            pinky.sendResponse(pRq);
            
            pRq = null;
        }
        
        /*
         * INSERT the next (not lost) entry (slave should request the lost entry!
         */
        
        final String nextKey = "nextKey";
        final String nextData = "nextValue";
        
        synchronized (testLock) {    
            PinkyRequest nextEntry;
            PinkyRequest lostRq;
            SpeedyRequest lostAnswer;
            LSN lostLSN;
            
            // asynchronous method-call for DB insert 
            Thread asyncInsert = new Thread(new Runnable() {
            
                @Override
                public void run() {                    
                    try {
                        master.syncSingleInsert(testDBName2, testIndexId, nextKey.getBytes(), nextData.getBytes());                       
                    } catch (BabuDBException e) {
                        fail("REPLICA should be inserted properly!");
                    }                     
                }
            });
            
            asyncInsert.start();
            
            
         // check the REPLICA from master
            while (pRq == null)
                testLock.wait();

            nextEntry = pRq;
            pRq = null;
            
            LogEntry logEntry = LogEntry.deserialize(ReusableBuffer.wrap(nextEntry.getBody()), checksum);
            checksum.reset();
            InsertRecordGroup irec = InsertRecordGroup.deserialize(logEntry.getPayload());                
            LSN lsn = logEntry.getLSN();
            
            lostLSN = actual;
            actual = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);
            
            assertEquals(Token.REPLICA,Token.valueOf(nextEntry.requestURI));
            assertEquals(actual, lsn);
            assertEquals(testIndexId,irec.getInserts().get(0).getIndexId());            
            assertEquals(nextKey,new String(irec.getInserts().get(0).getKey()));
            assertEquals(nextData,new String(irec.getInserts().get(0).getValue()));

         // send it to the slave      
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    nextEntry.requestURI,null,null,
                    ReusableBuffer.wrap(nextEntry.getBody()),
                    DATA_TYPE.BINARY), slave1_address); 
           
         // receive RQ for <<lost>> from the slave   
            while (pRq == null)
                testLock.wait();
            
            lostRq = pRq;
            pRq = null;
            
            assertEquals(Token.RQ,Token.valueOf(lostRq.requestURI));
            assertEquals(lostLSN,new LSN(new String(lostRq.getBody())));
            
         // send it to the master      
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    lostRq.requestURI,null,null,
                    ReusableBuffer.wrap(lostRq.getBody()),
                    DATA_TYPE.BINARY), master_address); 
            
         // check the response from master
            while (sRq == null)
                testLock.wait();
            
            lostAnswer = sRq;
            sRq = null;
            
            assertEquals(HTTPUtils.SC_OKAY, lostAnswer.statusCode);
            assertEquals(Token.RQ,Token.valueOf(lostAnswer.getURI()));
            logEntry = LogEntry.deserialize(ReusableBuffer.wrap(lostAnswer.getResponseBody()), checksum);
            checksum.reset();
            irec = InsertRecordGroup.deserialize(logEntry.getPayload());                
            lsn = logEntry.getLSN();         
            assertEquals(lostLSN, lsn);
            assertEquals(testIndexId,irec.getInserts().get(0).getIndexId());   
            assertEquals(lostKey,new String(irec.getInserts().get(0).getKey()));
            assertEquals(lostData,new String(irec.getInserts().get(0).getValue()));
            
         // send it to the slave
            lostRq.setResponse(HTTPUtils.SC_OKAY,ReusableBuffer.wrap(lostAnswer.getResponseBody()),DATA_TYPE.BINARY);
            pinky.sendResponse(lostRq);

         // wait for the ACK of the slave, and the HTTP_Okay for nextEntry
            while (sRq == null || pRq == null)
                testLock.wait();
            
         // send it to the master    
            assertEquals(Token.ACK,Token.valueOf(pRq.requestURI));
            assertEquals(lostLSN,new LSN(new String(pRq.getBody())));         
            
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    pRq.requestURI,null,null,
                    ReusableBuffer.wrap(pRq.getBody()),
                    DATA_TYPE.BINARY), master_address);
            
         // send it back to the master 
            assertEquals(HTTPUtils.SC_OKAY, sRq.statusCode);
            nextEntry.setResponse(HTTPUtils.SC_OKAY);
            pinky.sendResponse(nextEntry);
            
            sRq.freeBuffer();
            sRq = null;
            pRq = null;
            lostAnswer.freeBuffer();           
        }
    }
    
    /**
     * inserts 3 entries into the DB
     * 
     * @throws Exception
     */
    private void insertSomeData(final String dbName,final int index, final String[] keys, final String [] values) throws InterruptedException, LogEntryException, IllegalStateException, IOException{
        Checksum checksum = new CRC32();        
        
        /*
         * Insert entries (3)
         */
        for (int i=0;i<3;i++){
            synchronized (testLock) {
                          
                // asynchronous method-call for DB insert 
                Thread asyncInsert = new Thread(new Runnable() {
                
                    @Override
                    public void run() {
                        Random r = new Random();
                        
                        try {
                            master.syncSingleInsert(dbName, index, keys[r.nextInt(keys.length)].getBytes(), values[r.nextInt(values.length)].getBytes());
                        } catch (BabuDBException e) {
                            fail("Insert could not be replicated: "+e.getMessage());
                        }           
                    }
                });
                
                asyncInsert.start();
                
                
             // check the REPLICA from master
                while (pRq == null)
                    testLock.wait();
    
                LogEntry logEntry = LogEntry.deserialize(ReusableBuffer.wrap(pRq.getBody()), checksum);
                checksum.reset();
                InsertRecordGroup irec = InsertRecordGroup.deserialize(logEntry.getPayload());                
                LSN lsn = logEntry.getLSN();                
                actual = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);
                
                assertEquals(Token.REPLICA,Token.valueOf(pRq.requestURI));
                assertEquals(actual, lsn);
                assertEquals(index,irec.getInserts().get(0).getIndexId());
                boolean isIn = false;
                for (String k : keys){
                    if (isIn = (k.equals(new String(irec.getInserts().get(0).getKey())))) break;
                }
                
                assertTrue(isIn);
                
                for (String v : values){
                    if (isIn = (v.equals(new String(irec.getInserts().get(0).getValue())))) break;
                }
                
                assertTrue(isIn);
            
                speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                        pRq.requestURI,null,null,
                        ReusableBuffer.wrap(pRq.getBody()),
                        DATA_TYPE.BINARY), slave1_address); 
               
             // check the response from slave
                while (sRq == null)
                    testLock.wait();
                
                assertEquals(HTTPUtils.SC_OKAY, sRq.statusCode);
                
                sRq.freeBuffer();
                sRq = null;
             
             // send it back to the master   
                pRq.setResponse(HTTPUtils.SC_OKAY);
                pinky.sendResponse(pRq);
                
                pRq = null;
            }
        }
    }
    
    @Override
    public void receiveRequest(PinkyRequest theRequest) {
        synchronized (testLock) {
            pRq = theRequest;
            testLock.notify();
        }        
    }

    @Override
    public void receiveRequest(SpeedyRequest theRequest) {
        synchronized (testLock) {
            sRq = theRequest; 
            testLock.notify();
        }
    }
}
