/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;


import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.clients.SlaveClient;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaRequest;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.config.SlaveConfig;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.common.logging.Logging.Category;
import org.xtreemfs.include.foundation.LifeCycleListener;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.include.foundation.oncrpc.server.RPCNIOSocketServer;
import org.xtreemfs.include.foundation.oncrpc.server.RPCServerRequestListener;

import static org.xtreemfs.babudb.replication.TestData.*;

public class SlaveTest implements RPCServerRequestListener,LifeCycleListener {

    public final static int viewID = 1;
    public final static int MAX_MESSAGES_PRO_TEST = 10;
    
    private RPCNIOSocketServer      rpcServer;
    private static SlaveConfig      conf;
    private RPCNIOSocketClient      rpcClient;
    private SlaveClient             client;
    private BabuDB                  db;
    private LSN                     current = new LSN(0,0L);  
    private long                    replicaRangeLength = new Random().nextInt(100)+1L;
    private BlockingQueue<Integer>  mailbox;
     
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        conf = new SlaveConfig("config/slave.properties");
        conf.read();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        Process p = Runtime.getRuntime().exec("rm -rf " + conf.getBaseDir());
        assertEquals(0, p.waitFor());
        
        p = Runtime.getRuntime().exec("rm -rf " + conf.getDbLogDir());
        assertEquals(0, p.waitFor());

        p = Runtime.getRuntime().exec("rm -rf " + conf.getBackupDir());
        assertEquals(0, p.waitFor());
        
        mailbox = new ArrayBlockingQueue<Integer>(MAX_MESSAGES_PRO_TEST);
        try {
            assertTrue (conf.getSSLOptions() == null);
           
            rpcClient = new RPCNIOSocketClient(null,5000,10000);
            rpcClient.setLifeCycleListener(this);  
            client = new SlaveClient(rpcClient,new InetSocketAddress(conf.getAddress(),conf.getPort()));
            
            int port = conf.getMaster().getPort();
            InetAddress address = conf.getMaster().getAddress();
            rpcServer = new RPCNIOSocketServer(port,address,this,null);
            rpcServer.setLifeCycleListener(this);
            rpcServer.start();
            rpcServer.waitForStartup();
            
            db = BabuDBFactory.createSlaveBabuDB(conf);
            
            rpcClient.start();
            rpcClient.waitForStartup();
        } catch (Exception e) {
            System.err.println("BEFORE-FAILED: "+e.getMessage());
            throw e;
        }
    }
    
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
    public void testAwaitHeartbeat() throws Exception {
        System.out.println("Test: await heartbeat");
        awaitHeartbeat();
    }
    
    @Test
    public void testCreate() throws Exception {
        System.out.println("Test: create");
        makeDB();
    }
    
    @Test
    public void testReplicate() throws Exception {
        System.out.println("Test: replicate");
        makeDB();
        replicate();
    }
        
    @Test
    public void testCopy() throws Exception {
        System.out.println("Test: copy");
        makeDB();
        replicate();
        copyDB();
    }

    @Test
    public void testDelete() throws Exception {
        System.out.println("Test: delete");
        makeDB();
        replicate();
        copyDB();
        deleteDB();
    }
    
    @Test
    public void testReplicateFailure() throws Exception {
        System.out.println("Test: replicate failure");
        makeDB();
        replicate();
        provokeReplicateFailure();
    }

    @Test
    public void testLoad1() throws Exception {
        System.out.println("Test: load1");
        makeDB();
        replicate();
        provokeLoad1();
    }
    
    @Test
    public void testLoad2() throws Exception {
        System.out.println("Test: load2");
        makeDB();
        replicate();
        provokeLoad2();
    }
    
    private void awaitHeartbeat() throws Exception {
        assertEquals(heartbeatOperation, mailbox.take());
    }
    
    private void makeDB() throws Exception {
        RPCResponse<?> rp = client.create(new LSN(viewID,1L), testDB, testDBIndices);  
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        awaitHeartbeat();
    }
    
    private void copyDB() throws Exception {
        RPCResponse<?> rp = client.copy(new LSN(viewID,3L), testDB, copyTestDB);        
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        awaitHeartbeat();
    }
    
    private void deleteDB() throws Exception {
        RPCResponse<?> rp = client.delete(new LSN(viewID,4L), copyTestDB, true);
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        awaitHeartbeat();
    }
    
    private void replicate() throws Exception {
        final LSN testLSN = new LSN(viewID,2L);
        InsertRecordGroup ig = new InsertRecordGroup(testDBID);
        ig.addInsert(0, testKey1.getBytes(), testValue.getBytes());
        ig.addInsert(0, testKey2.getBytes(), testValue.getBytes());
        ig.addInsert(0, testKey3.getBytes(), testValue.getBytes());
        
        ReusableBuffer payload = new ReusableBuffer(ByteBuffer.allocate(ig.getSize()));
        ig.serialize(payload);
        payload.flip();
        LogEntry data = new LogEntry(payload , null);
        data.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
        
        RPCResponse<?> rp = client.replicate(testLSN, data.serialize(new CRC32()));
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        awaitHeartbeat();
    }
    
    private void provokeReplicateFailure() throws Exception {
        final LSN testLSN = new LSN(viewID,3L+replicaRangeLength);
        InsertRecordGroup ig = new InsertRecordGroup(testDBID);
        ig.addInsert(0, testKey1.getBytes(), testValue.getBytes());
        ig.addInsert(0, testKey2.getBytes(), testValue.getBytes());
        ig.addInsert(0, testKey3.getBytes(), testValue.getBytes());
        
        ReusableBuffer payload = new ReusableBuffer(ByteBuffer.allocate(ig.getSize()));
        ig.serialize(payload);
        payload.flip();
        LogEntry data = new LogEntry(payload , null);
        data.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
        
        RPCResponse<?> rp = client.replicate(testLSN, data.serialize(new CRC32()));
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        assertEquals(replicaOperation, mailbox.take());
    }
    
    private void provokeLoad1() throws Exception {
        final LSN testLSN = new LSN(viewID+1,3L);
        InsertRecordGroup ig = new InsertRecordGroup(testDBID);
        ig.addInsert(0, testKey1.getBytes(), testValue.getBytes());
        ig.addInsert(0, testKey2.getBytes(), testValue.getBytes());
        ig.addInsert(0, testKey3.getBytes(), testValue.getBytes());
        
        ReusableBuffer payload = new ReusableBuffer(ByteBuffer.allocate(ig.getSize()));
        ig.serialize(payload);
        payload.flip();
        LogEntry data = new LogEntry(payload , null);
        data.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
        
        RPCResponse<?> rp = client.replicate(testLSN, data.serialize(new CRC32()));
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        assertEquals(loadOperation, mailbox.take());
    }
    
    private void provokeLoad2() throws Exception {
        RPCResponse<?> rp = client.create(new LSN(viewID+1,1L), testDB, testDBIndices);  
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        assertEquals(loadOperation, mailbox.take());
    }
    
    @Override
    public void receiveRecord(ONCRPCRequest rq) {
        int opNum = rq.getRequestHeader().getProcedure();
        if (opNum == heartbeatOperation) {
            heartbeatRequest request = new heartbeatRequest();
            request.deserialize(rq.getRequestFragment());
            LSN lsn = new LSN(request.getLsn().getViewId(),
                    request.getLsn().getSequenceNo());
            assertTrue(lsn.compareTo(current)>0);
            current = lsn;
            
            rq.sendResponse(new heartbeatResponse());   
        } else if (opNum == replicaOperation) {
            replicaRequest request = new replicaRequest();
            request.deserialize(rq.getRequestFragment());
            LSNRange r = request.getRange();
            assertEquals(1, r.getViewId());
            assertEquals(replicaRangeLength-1L, r.getSequenceEnd()-r.getSequenceStart());
        } else if (opNum == loadOperation) {
            loadRequest request = new loadRequest();
            request.deserialize(rq.getRequestFragment());
            org.xtreemfs.babudb.interfaces.LSN lsn = request.getLsn();
            assertEquals(1, lsn.getViewId());
            assertEquals(2L,lsn.getSequenceNo());
        } else {
            rq.sendInternalServerError(new Throwable("DUMMY-REPLICATION"), new errnoException("DUMMY-REPLICATION"));
            fail("ERROR: received "+opNum);
        }
        mailbox.add(opNum);
    }

    @Override
    public void crashPerformed() { fail("Slave - client crashed!"); }

    @Override
    public void shutdownPerformed() {}

    @Override
    public void startupPerformed() {}
}
