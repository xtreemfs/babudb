/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.*;

import java.io.IOException;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.RequestPreProcessor.PreProcessException;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.logging.Logging;
import org.xtreemfs.foundation.json.JSONException;
import org.xtreemfs.foundation.json.JSONParser;
import org.xtreemfs.foundation.json.JSONString;
import org.xtreemfs.foundation.pinky.HTTPUtils;
import org.xtreemfs.foundation.pinky.PinkyRequest;
import org.xtreemfs.foundation.pinky.HTTPUtils.DATA_TYPE;
import org.xtreemfs.foundation.speedy.SpeedyRequest;

/*
import java.util.Map;
import org.xtreemfs.babudb.replication.Missing.STATUS;
import java.util.LinkedList;
import java.util.HashMap;
*/

public class RequestPreProcessorTest {
    private LSMDBRequest context;
    
    private DummyReplication replicationFacade;
    
    public RequestPreProcessorTest() {
        Logging.start(Logging.LEVEL_ERROR);
    }
    
    @Before
    public void setUp() throws Exception {      
        
        context = new LSMDBRequest(null,0,new BabuDBRequestListener() {
        
            @Override
            public void userDefinedLookupFinished(Object context, Object result) {}
        
            @Override
            public void requestFailed(Object context, BabuDBException error) {}
        
            @Override
            public void prefixLookupFinished(Object context,
                    Iterator<Entry<byte[], byte[]>> iterator) {}
        
            @Override
            public void lookupFinished(Object context, byte[] value) {}
        
            @Override
            public void insertFinished(Object context) {}
        },null,false,null);
        
        replicationFacade = new DummyReplication(null,35666);
    }

    @After
    public void tearDown() throws Exception {
        replicationFacade.stop();
    }

    @Test
    public void testReplicationBROADCAST() throws PreProcessException, LogEntryException, IOException{
        String testData = "PAYLOAD";
        LSN testLSN = new LSN(1,1L);  
        LogEntry testLogEntry = new LogEntry(ReusableBuffer.wrap(testData.getBytes()),null);
        testLogEntry.setAttachment(context);
        testLogEntry.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
       
        /*
         * REPLICA_BROADCAST
         */
        
        Request broadcastRQ = RequestPreProcessor.getReplicationRequest(testLogEntry);       
        assertNull(broadcastRQ.getChunkDetails());
        assertNull(broadcastRQ.getLsmDbMetaData());
        assertNull(broadcastRQ.getOriginal());
        assertNull(broadcastRQ.getSource());
        assertEquals(Token.REPLICA_BROADCAST, broadcastRQ.getToken());
        assertEquals(testLSN,broadcastRQ.getLSN());
        assertEquals(context,broadcastRQ.getContext());
        
        LogEntry rqLe = LogEntry.deserialize(broadcastRQ.getData(), new CRC32());
        assertEquals(testLogEntry.getLSN(), rqLe.getLSN()); 
        assertEquals(testData,new String(rqLe.getPayload().array()));
        rqLe.free();
        testLogEntry.free();
        
        /*
         * REPLICA - inactive, because of the DB - check 
         */
        
        testLogEntry = new LogEntry(ReusableBuffer.wrap(testData.getBytes()),null);
        testLogEntry.setAttachment(context);
        testLogEntry.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
        /*        PinkyRequest testREPLICA = new PinkyRequest(HTTPUtils.POST_TOKEN,Token.REPLICA.toString(),null,null, testLogEntry.serialize(new CRC32()));       
        Request rq = RequestPreProcessor.getReplicationRequest(testREPLICA,replicationApproach);
        assertEquals(Token.REPLICA, rq.getToken());
        assertEquals(testLSN,rq.getLSN());
        assertEquals(testData,new String(rq.getLogEntry().getPayload().array()));   */
        
        /*
         * ACK as sub request
         */
        
        SpeedyRequest testREPLICAResponse = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.REPLICA.toString(),null,null,testLogEntry.serialize(new CRC32()),DATA_TYPE.BINARY);        
        testREPLICAResponse.genericAttatchment = new Status<Request>(broadcastRQ);
        Request rq = RequestPreProcessor.getReplicationRequest(testREPLICAResponse,replicationFacade);
        assertEquals(Token.ACK, rq.getToken());
        assertEquals(testLSN,rq.getLSN()); 
        
        testLogEntry.free();
        rq.free();
        broadcastRQ.free();
        testREPLICAResponse.freeBuffer();
    }
/*    due to security mechanisms these tests are not available anymore


    @Test
    public void testReplicationRQ() throws PreProcessException{
        LSN testLSN = new LSN(1,1L);  
        
        /*
         * RQ
         /
        
        PinkyRequest testRQ = new PinkyRequest(HTTPUtils.POST_TOKEN,Token.RQ.toString(),null,null,ReusableBuffer.wrap(testLSN.toString().getBytes()));       
        Request rq = RequestPreProcessor.getReplicationRequest(testRQ,replicationApproach);
        assertEquals(Token.RQ, rq.getToken());
        assertEquals(testLSN,rq.getLSN());
        rq.free();
    
        /*
         * REPLICAResponse -> same as in testReplicationBROADCAST()
         /
        
        /*
         * ACK
         /
        
        PinkyRequest testACK = new PinkyRequest(HTTPUtils.POST_TOKEN,Token.ACK.toString(),null,null,ReusableBuffer.wrap(testLSN.toString().getBytes()));
        rq = RequestPreProcessor.getReplicationRequest(testACK,replicationApproach);
        assertEquals(Token.ACK, rq.getToken());
        assertEquals(testLSN,rq.getLSN()); 
        
        rq.free();
    }
    
    @Test
    public void testReplicationLOAD() throws PreProcessException, JSONException{ 
        Map<String, List<Long>> testLSMDBData = new HashMap<String, List<Long>>();
        List<Long> data = new LinkedList<Long>();
        data.add(100L); data.add(1000L);
        testLSMDBData.put("fileName", data);
        
        /*
         * LOAD
         /
        
        PinkyRequest testLOAD = new PinkyRequest(HTTPUtils.POST_TOKEN,Token.LOAD.toString(),null,null,null);       
        Request rq = RequestPreProcessor.getReplicationRequest(testLOAD,replicationApproach);
        assertEquals(Token.LOAD, rq.getToken()); 
        rq.free();
        
        /*
         * LOAD_RP
         /
        
        SpeedyRequest testLOADResponse = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.LOAD.toString(),null,null,null,DATA_TYPE.JSON);        
        testLOADResponse.responseBody = ReusableBuffer.wrap(JSONParser.writeJSON(testLSMDBData).getBytes());
        rq = RequestPreProcessor.getReplicationRequest(testLOADResponse,replicationApproach);
        assertEquals(Token.LOAD_RP, rq.getToken());
        Map<String, List<Long>> load = (Map<String, List<Long>>) rq.getLsmDbMetaData();
        assertNotNull(load);
        List<Long> loadedData = load.get("fileName");
        assertNotNull(loadedData);
        assertEquals(100L, loadedData.get(0));
        assertEquals(1000L, loadedData.get(1));
        rq.free();
    }
    
    @Test
    public void testReplicationCHUNK() throws PreProcessException, JSONException{
        Chunk testChunk = new Chunk("testFileName",0L,100L);      
        Missing<Chunk> testContext = new Missing<Chunk>(testChunk, STATUS.PENDING);
        String testData = "PAYLOAD";
        
        /*
         * CHUNK    
         /
        
        PinkyRequest testCHUNK = new PinkyRequest(HTTPUtils.POST_TOKEN,Token.CHUNK.toString(),null,null,ReusableBuffer.wrap(JSONParser.writeJSON(testChunk.toJSON()).getBytes()));       
        Request rq = RequestPreProcessor.getReplicationRequest(testCHUNK,replicationApproach);
        assertEquals(Token.CHUNK, rq.getToken());
        assertEquals(testChunk,rq.getChunkDetails()); 
        rq.free();
        
        /*
         * CHUNK_RP
         /
        
        SpeedyRequest testCHUNKResponse = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.CHUNK.toString(),null,null,null,DATA_TYPE.BINARY);        
        testCHUNKResponse.genericAttatchment = testContext;
        testCHUNKResponse.responseBody = ReusableBuffer.wrap(testData.getBytes());
        rq = RequestPreProcessor.getReplicationRequest(testCHUNKResponse,replicationApproach);
        assertEquals(Token.CHUNK_RP, rq.getToken());
        assertEquals(testData,new String(rq.getData().array()));   
        rq.free();
    }
 */   
    @Test
    public void testGetReplicationRequestFailures() throws PreProcessException, JSONException, IOException, LogEntryException {
        Request rq = null;
        
        /*
         * Error cases
         */
        
        /*
         * LogEntry
         */
        
        try {
            rq = RequestPreProcessor.getReplicationRequest(null);
            fail("Null logEntry should cast an exception.");
        }catch(PreProcessException e){
            assertTrue(true);
        } 
        
        /*
         * Pinky
         */
        
        PinkyRequest testDefectToken = new PinkyRequest(HTTPUtils.POST_TOKEN,"defectToken",null,null,null);
        try {
            rq = RequestPreProcessor.getReplicationRequest(testDefectToken,replicationFacade);
            fail("Defect Token should cast an exception.");
        }catch(PreProcessException e){
            assertTrue(true);
        } 
        
        PinkyRequest testFalseToken = new PinkyRequest(HTTPUtils.POST_TOKEN,Token.REPLICA_BROADCAST.toString(),null,null,null);
        try {
            rq = RequestPreProcessor.getReplicationRequest(testFalseToken,replicationFacade);
            fail("Defect Token should cast an exception.");
        }catch(PreProcessException e){
            assertTrue(true);
        } 
        
        /*
         * Speedy
         */
        
        SpeedyRequest testDefectTokenS = new SpeedyRequest(HTTPUtils.POST_TOKEN,"defectToken",null,null,null,DATA_TYPE.BINARY);
        try {
            rq = RequestPreProcessor.getReplicationRequest(testDefectTokenS,replicationFacade);
            fail("Defect Token should cast an exception.");
        }catch(PreProcessException e){
            assertTrue(true);
        } 
        
        SpeedyRequest testFalseTokenS = new SpeedyRequest(HTTPUtils.POST_TOKEN,Token.REPLICA_BROADCAST.toString(),null,null,null,DATA_TYPE.BINARY);
        try {
            rq = RequestPreProcessor.getReplicationRequest(testFalseTokenS,replicationFacade);
            fail("Defect Token should cast an exception.");
        }catch(PreProcessException e){
            assertTrue(true);
        } 
        
        assertNull(rq);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetReplicationRequestCREATE() throws PreProcessException, JSONException {
        String testDBName = "testDB";
        int testNumIndices = 5;
        
        Request rq = RequestPreProcessor.getReplicationRequest(testDBName, testNumIndices);
        assertEquals(Token.CREATE,rq.getToken());
        List<Object> load = (List<Object>) JSONParser.parseJSON(new JSONString(new String(rq.getData().array())));
        assertEquals(testDBName,(String) load.get(0));
        assertEquals(testNumIndices,Integer.parseInt((String) load.get(1)));
        rq.free();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetReplicationRequestCOPY() throws PreProcessException, JSONException {
        String testSource = "testSourceDB";
        String testDestination = "testDestinationDB";
        
        Request rq = RequestPreProcessor.getReplicationRequest(testSource, testDestination);
        assertEquals(Token.COPY,rq.getToken());
        List<Object> load = (List<Object>) JSONParser.parseJSON(new JSONString(new String(rq.getData().array())));
        assertEquals(testSource,(String) load.get(0));
        assertEquals(testDestination,(String) load.get(1));
        rq.free();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetReplicationRequestDELETE() throws PreProcessException, JSONException {
        String testDBName = "testDB";
        boolean testDeleteAll = false;
        
        Request rq = RequestPreProcessor.getReplicationRequest(testDBName, testDeleteAll);
        assertEquals(Token.DELETE,rq.getToken());
        List<Object> load = (List<Object>) JSONParser.parseJSON(new JSONString(new String(rq.getData().array())));
        assertEquals(testDBName,(String) load.get(0));
        assertEquals(testDeleteAll,Boolean.valueOf((String) load.get(1)));
        rq.free();
    }
}
