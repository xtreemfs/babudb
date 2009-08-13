/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.xtreemfs.babudb.replication.TestData.copyOperation;
import static org.xtreemfs.babudb.replication.TestData.copyTestDB;
import static org.xtreemfs.babudb.replication.TestData.createOperation;
import static org.xtreemfs.babudb.replication.TestData.deleteOperation;
import static org.xtreemfs.babudb.replication.TestData.replicateOperation;
import static org.xtreemfs.babudb.replication.TestData.testDB;
import static org.xtreemfs.babudb.replication.TestData.testDBID;
import static org.xtreemfs.babudb.replication.TestData.testDBIndices;
import static org.xtreemfs.babudb.replication.TestData.testKey1;
import static org.xtreemfs.babudb.replication.TestData.testKey2;
import static org.xtreemfs.babudb.replication.TestData.testKey3;
import static org.xtreemfs.babudb.replication.TestData.testValue;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDBRequestListener;
import org.xtreemfs.babudb.clients.MasterClient;
import org.xtreemfs.babudb.interfaces.Chunk;
import org.xtreemfs.babudb.interfaces.DBFileMetaData;
import org.xtreemfs.babudb.interfaces.DBFileMetaDataSet;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.LogEntries;
import org.xtreemfs.babudb.interfaces.Exceptions.errnoException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.copyRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.copyResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.createRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.createResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.deleteRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.deleteResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicateResponse;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.replication.operations.ErrNo;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.common.logging.Logging.Category;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.include.foundation.oncrpc.server.RPCNIOSocketServer;
import org.xtreemfs.include.foundation.oncrpc.server.RPCServerRequestListener;

public class MasterTest implements RPCServerRequestListener,LifeCycleListener{
        
    private static final int viewID = 1;
    
    private RPCNIOSocketServer  rpcServer;
    private static MasterConfig conf;
    private RPCNIOSocketClient  rpcClient;
    private MasterClient        client;
    private BabuDB              db;
    private final AtomicInteger response = new AtomicInteger(-1);
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        conf = new MasterConfig("config/master.properties");
        conf.read();
    }
    
    @Before
    public void setUp() throws Exception { 
        Process p = Runtime.getRuntime().exec("rm -rf " + conf.getBaseDir());
        assertEquals(0, p.waitFor());
        
        p = Runtime.getRuntime().exec("rm -rf " + conf.getDbLogDir());
        assertEquals(0, p.waitFor());
        
        try {
            db = BabuDBFactory.createMasterBabuDB(conf);
            assert (!conf.isUsingSSL());
            rpcClient = new RPCNIOSocketClient(null,5000,10000);
            rpcClient.setLifeCycleListener(this);
            client = new MasterClient(rpcClient,new InetSocketAddress(conf.getAddress(),conf.getPort()));
           
            int port = conf.getSlaves().get(0).getPort();
            InetAddress address = conf.getSlaves().get(0).getAddress();
            rpcServer = new RPCNIOSocketServer(port,address,this,null);
            rpcServer.setLifeCycleListener(this);
            
            rpcClient.start();
            rpcServer.start();
            
            rpcClient.waitForStartup();
            rpcServer.waitForStartup();
        } catch (Exception e) {
            System.err.println("BEFORE-FAILED: "+e.getMessage());
            throw e;
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @After
    public void tearDown() throws Exception {
        try {
            db.shutdown();
            
            rpcClient.shutdown();
            rpcServer.shutdown();
            
            rpcClient.waitForShutdown();
            rpcServer.waitForShutdown();
        } catch (Exception e){
            System.err.println("AFTER-FAILED: "+e.getMessage());
            throw e;
        }
    }
    
    @Test
    public void testHeartBeat () {
        System.out.println("Test: heartbeat");
        dummyHeartbeat(0);
    }
    
    @Test
    public void testCreate() throws Exception{
        System.out.println("Test: create");
        makeDB();
    }
    
    @Test
    public void testReplicate() throws Exception{
        System.out.println("Test: replicate");
        makeDB();
        insertData();
    }
    
    @Test 
    public void testCopy() throws Exception {
        System.out.println("Test: copy");
        makeDB();
        insertData();
        copyDB();
    }
    
    @Test 
    public void testDelete() throws Exception {
        System.out.println("Test: delete");
        makeDB();
        insertData();
        copyDB();
        deleteDB();
    }
    
    @Test
    public void testgetReplica() throws Exception {
        System.out.println("Test: request");
        makeDB();
        insertData();
        
        long seqToRequest = 2L;
        
        RPCResponse<LogEntries> result = client.getReplica(new LSNRange(1,seqToRequest,seqToRequest));
        LogEntries les = result.get();
        assertNotNull(les);
        assertEquals(1, les.size());
        LogEntry le = LogEntry.deserialize(les.get(0).getPayload(), new CRC32());
        assertEquals(viewID, le.getViewId());
        assertEquals(seqToRequest, le.getLogSequenceNo());
        
        assertEquals(viewID, le.getLSN().getViewId());
        assertEquals(seqToRequest, le.getLSN().getSequenceNo());
        
        assertNotNull(le.getPayload());
        
        le.free();
    }
    
    @Test
    public void testgetReplicaUnavailable() throws Exception {
        System.out.println("Test: request-unavailable");
        makeDB();
        insertData();
        
        long seqToRequest = 1L;
        
        RPCResponse<LogEntries> result = client.getReplica(new LSNRange(1,seqToRequest,seqToRequest));
        try {
            result.get();
        } catch (errnoException e) {
            assertEquals(ErrNo.SERVICE_CALL_MISSED.ordinal(), e.getError_code());
        }
    }
    
    @Test
    public void testInitialLoad() throws Exception {
        System.out.println("Test: load");
        makeDB();
        insertData();
        
        RPCResponse<DBFileMetaDataSet> result = client.load(new LSN(1,0L));
        
        DBFileMetaDataSet fMDatas = result.get();
        assertNotNull(fMDatas);
        assertEquals(1, fMDatas.size());
        for (DBFileMetaData metaData : fMDatas) {
            long size = new File(conf.getBaseDir()+conf.getDbCfgFile()).length();
            assertEquals(conf.getBaseDir()+conf.getDbCfgFile(), metaData.getFileName());
            assertEquals(size, metaData.getFileSize());
            assertEquals(conf.getChunkSize(),metaData.getMaxChunkSize());
        
            RPCResponse<ReusableBuffer> chunkRp = client.chunk(new Chunk(metaData.getFileName(), 0L, metaData.getFileSize()));
            
            ReusableBuffer buf = chunkRp.get();
            assertEquals(size, buf.capacity());
            
            BufferPool.free(buf);
        }
    }

    @Override
    public void receiveRecord(ONCRPCRequest rq) {
        int opNum = rq.getRequestHeader().getOperationNumber();
        final Checksum chksm = new CRC32();
        
        synchronized (response) {
            if (opNum == replicateOperation) {
                replicateRequest request = new replicateRequest();
                request.deserialize(rq.getRequestFragment());
                assertEquals(1, request.getLsn().getViewId());
                assertEquals(2L,request.getLsn().getSequenceNo());
                
                LogEntry le = null;
                try {
                    le = LogEntry.deserialize(request.getLogEntry().getPayload(), chksm);
                    assertEquals(viewID, le.getViewId());
                    assertEquals(2L,le.getLogSequenceNo());
                    
                    InsertRecordGroup ig = InsertRecordGroup.deserialize(le.getPayload());
                    assertEquals(testDBID,ig.getDatabaseId());
                    
                    List<InsertRecord> igs = ig.getInserts();
                    InsertRecord ir = igs.get(0);
                    assertEquals(0, ir.getIndexId());
                    assertEquals(testKey1, new String(ir.getKey()));
                    assertEquals(testValue, new String(ir.getValue()));
                    
                    ir = igs.get(1);
                    assertEquals(0, ir.getIndexId());
                    assertEquals(testKey2, new String(ir.getKey()));
                    assertEquals(testValue, new String(ir.getValue()));
                    
                    ir = igs.get(2);
                    assertEquals(0, ir.getIndexId());
                    assertEquals(testKey3, new String(ir.getKey()));
                    assertEquals(testValue, new String(ir.getValue()));
                } catch (LogEntryException e) {
                    fail();
                } finally {
                    chksm.reset();
                    if (le!=null) le.free();
                }
                
                rq.sendResponse(new replicateResponse());
            } else if (opNum == createOperation) {
                createRequest request = new createRequest();
                request.deserialize(rq.getRequestFragment());
                assertEquals(viewID, request.getLsn().getViewId());
                assertEquals(1L,request.getLsn().getSequenceNo());
                assertEquals(testDB,request.getDatabaseName());
                assertEquals(testDBIndices,request.getNumIndices());

                rq.sendResponse(new createResponse());
            } else if (opNum == copyOperation) {
                copyRequest request = new copyRequest();
                request.deserialize(rq.getRequestFragment());
                assertEquals(viewID, request.getLsn().getViewId());
                assertEquals(3L,request.getLsn().getSequenceNo());
                assertEquals(testDB,request.getSourceDB());
                assertEquals(copyTestDB,request.getDestDB());

                rq.sendResponse(new copyResponse());
            } else if (opNum == deleteOperation) {
                deleteRequest request = new deleteRequest();
                request.deserialize(rq.getRequestFragment());
                assertEquals(viewID, request.getLsn().getViewId());
                assertEquals(4L,request.getLsn().getSequenceNo());
                assertEquals(copyTestDB,request.getDatabaseName());
                assertEquals(true,request.getDeleteFiles());

                rq.sendResponse(new deleteResponse());
            } else {
                rq.sendInternalServerError(new Throwable("TEST-DUMMY-RESPONSE"));
                fail();
            }   
            response.set(opNum);
            response.notify();
        }
    }
    
    private void dummyHeartbeat(long sequence) {
        client.heartbeat(new LSN(viewID,sequence));
    }
    
    private void makeDB() throws Exception {
        synchronized (response) {
            db.getDatabaseManager().createDatabase(testDB, testDBIndices);
            
            while (response.get()!=createOperation)
                response.wait();
            
            response.set(-1);
        }
    }
    
    private void copyDB() throws Exception {
        synchronized (response) {
            db.getDatabaseManager().copyDatabase(testDB, copyTestDB, null, null);
            
            while (response.get()!=copyOperation)
                response.wait();
            
            response.set(-1);
        }
    }
    
    private void deleteDB() throws Exception {
        synchronized (response) {
            db.getDatabaseManager().deleteDatabase(copyTestDB, true);
            
            while (response.get()!=deleteOperation)
                response.wait();
            
            response.set(-1);
        }
    }
    
    private void insertData() throws Exception {
        Database dbase = db.getDatabaseManager().getDatabase(testDB);
        BabuDBInsertGroup testInsert = dbase.createInsertGroup();
        testInsert.addInsert(0, testKey1.getBytes(), testValue.getBytes());
        testInsert.addInsert(0, testKey2.getBytes(), testValue.getBytes());
        testInsert.addInsert(0, testKey3.getBytes(), testValue.getBytes());
           
        synchronized (response) {
            dbase.asyncInsert(testInsert, new BabuDBRequestListener() {
            
                @Override
                public void userDefinedLookupFinished(Object context, Object result) {
                    fail();
                }
            
                @Override
                public void requestFailed(Object context, BabuDBException error) {
                    fail();
                }
            
                @Override
                public void prefixLookupFinished(Object context,
                        Iterator<Entry<byte[], byte[]>> iterator) {
                    fail();
                }
            
                @Override
                public void lookupFinished(Object context, byte[] value) {
                    fail();
                }
            
                @Override
                public void insertFinished(Object context) {
                    assertTrue(true);
                }
            }, null);
            
            while (response.get()!=replicateOperation)
                response.wait();
            
            response.set(-1);
        }
    }

    @Override
    public void crashPerformed() { fail("Master crashed!"); }

    @Override
    public void shutdownPerformed() { }

    @Override
    public void startupPerformed() { }
}
