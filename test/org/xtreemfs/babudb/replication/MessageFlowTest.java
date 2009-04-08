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
    
	public final static int NO_ENTRIES = 1;
	
    private static LSN actual = new LSN(1,0L);
    
    private static MultiSpeedy speedy;
    
    private static PipelinedPinky pinky;

    private static BabuDBImpl master;
    
    private static BabuDBImpl slave;
    
    private PinkyRequest pRq = null;
    
    private Boolean sRpState = null;
    
    private final Object testLock = new Object();  
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {   
        Logging.start(Logging.LEVEL_ERROR);
        
        Process p = Runtime.getRuntime().exec("rm -rf "+master_baseDir);
        p.waitFor();
        
        p = Runtime.getRuntime().exec("rm -rf "+slave1_baseDir);
        p.waitFor();
        
        InetSocketAddress snifferAddress = new InetSocketAddress("localhost",PORT);
        
        // start the communication components (standing between master and slave and verifying their communication)
        pinky = new PipelinedPinky(snifferAddress.getPort(),snifferAddress.getAddress(),null);
        pinky.start();
        pinky.waitForStartup();
        
        speedy = new MultiSpeedy();
        speedy.start();
        speedy.waitForStartup();
        
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();
        slaves.add(snifferAddress);
        
        Logging.logMessage(Logging.LEVEL_TRACE, slave, "starting up databases...");
        
        // start the master - SYNC replication mode       
        master = (BabuDBImpl) BabuDBFactory.getMasterBabuDB(master_baseDir,master_baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0,slaves,0,null,slaves.size(),0);
        
        // start the slave
        slave = (BabuDBImpl) BabuDBFactory.getSlaveBabuDB(slave1_baseDir,slave1_baseDir,1,0,0,SyncMode.SYNC_WRITE,0,0,snifferAddress,slaves,0,null,0);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Logging.logMessage(Logging.LEVEL_TRACE, slave, "shutting down databases...");
        pinky.shutdown();
        speedy.shutdown();
        master.shutdown();
        slave.shutdown();
        pinky.waitForShutdown();
        speedy.waitForShutdown();
    }

    @SuppressWarnings("unchecked")
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
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
         
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
    
    @SuppressWarnings("unchecked")
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
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
         
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
    
    @SuppressWarnings("unchecked")
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
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
         
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
        
        InetSocketAddress unavailableSlave = new InetSocketAddress(unknown_host,12345);
        // tests if the unavailable slave is really unavailable
        assertNull(unavailableSlave.getAddress());
        
        final List<InetSocketAddress> testDummySlave = new LinkedList<InetSocketAddress>();
        testDummySlave.add(slave1_address);
        testDummySlave.add(unavailableSlave);
        
        master.replicationFacade.setSlaves(testDummySlave);
        
        // apply the replication mode (SYNC)
        master.replicationFacade.setSyncMode(testDummySlave.size());
        
        /*
         * INSERT the lost entry
         */
        
        try {
            master.syncSingleInsert(testDBName2, testIndexId, lostKey.getBytes(), lostData.getBytes());    
            fail("REPLICA should fail!");
        } catch (BabuDBException e) {}     
        
        // catch the ACK rq's and send the responses to slave1
        actual = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);

        synchronized (testLock) {
            while (pRq == null)
                testLock.wait();
            
            assertEquals(Token.ACK,Token.valueOf(pRq.requestURI));
            assertEquals(actual,new LSN(new String(pRq.getBody())));
            
            // send ACK to the slave1    
            pRq.setResponse(HTTPUtils.SC_OKAY);
            pinky.sendResponse(pRq);
            
            pRq = null;
        }
        
        master.replicationFacade.setSlaves(reset);
    }
    
    @Test
    public void replicaRQTest () throws InterruptedException, LogEntryException, IllegalStateException, IOException, BabuDBException {
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
            
            // apply the replication mode (ASYNC)
            master.replicationFacade.setSyncMode(0);
            
            // asynchronous method-call for DB insert 
            Thread asyncInsert = new Thread(new Runnable() {
            
                @Override
                public void run() {                    
                    try {
                        master.syncSingleInsert(testDBName2, testIndexId, lostKey.getBytes(), lostData.getBytes());                      
                    } catch (BabuDBException e) {
                        fail("REPLICA should not have got lost!");
                    }                     
                }
            });
            
            asyncInsert.start();
            
            
         // check the REPLICA from master/ACK from slave
            while (pRq == null)
                testLock.wait();

            LogEntry logEntry = LogEntry.deserialize(ReusableBuffer.wrap(pRq.getBody()), checksum);
            checksum.reset();
            InsertRecordGroup irec = InsertRecordGroup.deserialize(logEntry.getPayload());                
            LSN lsn = logEntry.getLSN(); 
            
            LSN illegalLSN = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);
            
            assertEquals(Token.REPLICA,Token.valueOf(pRq.requestURI));
            assertEquals(illegalLSN, lsn);
            assertEquals(testIndexId,irec.getInserts().get(0).getIndexId());            
            assertTrue(lostKey.equals(new String(irec.getInserts().get(0).getKey())));            
            assertTrue(lostData.equals(new String(irec.getInserts().get(0).getValue())));

          // --> DO NOT SEND IT TO THE SLAVE! IT IS LOST
            actual = new LSN(actual.getViewId(),actual.getSequenceNo()+1L);
            
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
            PinkyRequest lostAnswer;
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
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
            
            while (pRq == null)
            	testLock.wait();
            
            lostAnswer = pRq;
            pRq = null;
                       
            assertEquals(Token.REPLICA,Token.valueOf(lostAnswer.requestURI));
            logEntry = LogEntry.deserialize(ReusableBuffer.wrap(lostAnswer.getBody()), checksum);
            checksum.reset();
            irec = InsertRecordGroup.deserialize(logEntry.getPayload());                
            lsn = logEntry.getLSN();         
            assertEquals(lostLSN, lsn);
            assertEquals(testIndexId,irec.getInserts().get(0).getIndexId());   
            assertEquals(lostKey,new String(irec.getInserts().get(0).getKey()));
            assertEquals(lostData,new String(irec.getInserts().get(0).getValue()));
            
         // send it to the slave
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
            		lostAnswer.requestURI,null,null,
            		ReusableBuffer.wrap(lostAnswer.getBody()),
            		DATA_TYPE.BINARY), slave1_address);
          
         // check the response of the slave
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
            
         // wait for the ACKs of the slave for nextEnry and lostEntry 
            while (pRq == null)
            	testLock.wait();
                        
         // send it to the master    
            boolean actualFirst = false;
            
            assertEquals(Token.ACK,Token.valueOf(pRq.requestURI));
            
            if (!lostLSN.equals(new LSN(new String(pRq.getBody())))){
            	assertEquals(actual,new LSN(new String(pRq.getBody()))); //TODO
            	actualFirst = true;
            }
            
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    pRq.requestURI,null,null,
                    ReusableBuffer.wrap(pRq.getBody()),
                    DATA_TYPE.BINARY), master_address);
         
            pRq = null;
            
         // check the response of the master
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
            
         // wait for the ACKs of the slave for nextEnry and lostEntry 
            while (pRq == null)
            	testLock.wait();
                        
         // send it to the master    
            assertEquals(Token.ACK,Token.valueOf(pRq.requestURI));
            if (actualFirst) {
            	assertEquals(lostLSN,new LSN(new String(pRq.getBody())));
            } else {
            	assertEquals(actual,new LSN(new String(pRq.getBody())));         
            }
            
            speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                    pRq.requestURI,null,null,
                    ReusableBuffer.wrap(pRq.getBody()),
                    DATA_TYPE.BINARY), master_address);
            
            // check the response of the master
            while (sRpState == null)
                testLock.wait();
            
            assertTrue(sRpState);
            sRpState = null;
            
            pRq = null;           
        }
    }
    
    /**
     * inserts NO_ENTRIES entries into the DB
     * 
     * @throws Exception
     */
    private void insertSomeData(final String dbName,final int index, final String[] keys, final String [] values) throws InterruptedException, LogEntryException, IllegalStateException, IOException{
        Checksum checksum = new CRC32();        
        
        /*
         * Insert entries
         */
        for (int i=0;i<NO_ENTRIES;i++){
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
               
             // check the response of the slave
                while (sRpState == null)
                    testLock.wait();
                
                assertTrue(sRpState);
                sRpState = null;
             
             // send it back to the master   
                pRq.setResponse(HTTPUtils.SC_OKAY);
                pinky.sendResponse(pRq);
                
                pRq = null;
                
             // wait for the ACK of the slave
                while (pRq == null)
                    testLock.wait();
                
                assertEquals(Token.ACK,Token.valueOf(pRq.requestURI));
                assertEquals(actual,new LSN(new String(pRq.getBody())));
                
             // redirect it to the master
                speedy.sendRequest(new SpeedyRequest(HTTPUtils.POST_TOKEN,
                        pRq.requestURI,null,null,
                        ReusableBuffer.wrap(pRq.getBody()),
                        DATA_TYPE.BINARY), master_address);
                
             // check the response of the master
                while (sRpState == null)
                    testLock.wait();
                
                assertTrue(sRpState);
                sRpState = null;
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
	    	sRpState = (theRequest.statusCode==HTTPUtils.SC_OKAY);
	    	testLock.notify();
	    }
    	theRequest.freeBuffer();
    }
}
