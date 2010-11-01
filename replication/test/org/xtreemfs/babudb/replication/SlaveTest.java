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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.interfaces.LSNRange;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.errnoException;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.heartbeatResponse;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.loadRequest;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.replicaRequest;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.SyncListener;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.transmission.client.Client;
import org.xtreemfs.babudb.replication.transmission.client.InterfaceExceptionParser;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RemoteExceptionParser;
import org.xtreemfs.foundation.oncrpc.server.NullAuthFlavorProvider;
import org.xtreemfs.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.foundation.oncrpc.server.RPCNIOSocketServer;
import org.xtreemfs.foundation.oncrpc.server.RPCServerRequestListener;
import org.xtreemfs.foundation.oncrpc.utils.XDRUnmarshaller;

import static org.xtreemfs.babudb.replication.TestData.*;

public class SlaveTest implements RPCServerRequestListener,LifeCycleListener {
    
    public final static boolean WIN = System.getProperty("os.name").toLowerCase().contains("win");
    
    public final static int viewID = 1;
    public final static int MAX_MESSAGES_PRO_TEST = 10;
    
    private final static Checksum    csumAlgo = new CRC32();
    private RPCNIOSocketServer       rpcServer;
    private static ReplicationConfig conf;
    private RPCNIOSocketClient       rpcClient;
    private Client              client;
    private BabuDB                   db;
    public LSN                       current;  
    private long                     replicaRangeLength = new Random().nextInt(100)+1L;
    private BlockingQueue<Integer>   mailbox;
     
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        conf = new ReplicationConfig("config/replication.properties");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
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
                
        current = new LSN(0,0L);
        mailbox = new ArrayBlockingQueue<Integer>(MAX_MESSAGES_PRO_TEST);
        try {
            assertTrue (conf.getSSLOptions() == null);
           
            rpcClient = new RPCNIOSocketClient(null,5000,10000, 
                    new RemoteExceptionParser[]{new InterfaceExceptionParser()});
            rpcClient.setLifeCycleListener(this);  
            client = new Client(rpcClient,conf.getInetSocketAddress(),null);
            
            List<InetSocketAddress> openAddresses = 
                new LinkedList<InetSocketAddress>(conf.getParticipants());
            int port = openAddresses.get(0).getPort();
            InetAddress address = openAddresses.get(0).getAddress();
            rpcServer = new RPCNIOSocketServer(port,address,this,null,
                            new NullAuthFlavorProvider());
            rpcServer.setLifeCycleListener(this);
            rpcServer.start();
            rpcServer.waitForStartup();
            
            db = BabuDBFactory.createReplicatedBabuDB(conf,null);
            
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
        awaitHeartbeat();
        provokeReplicateFailure();
    }

    @Test
    public void testLoad1() throws Exception {
        System.out.println("Test: load1");
        makeDB();
        replicate();
        awaitHeartbeat();
        provokeLoad1();
    }
    
    private void awaitHeartbeat() throws Exception {
        assertEquals(heartbeatOperation, (int) mailbox.take());
    }
    
    private void makeDB() throws Exception {
        ReusableBuffer buf = ReusableBuffer.wrap(
                new byte[(Integer.SIZE*3/8)+testDB.getBytes().length]);
        buf.putInt(1);
        buf.putString(testDB);
        buf.putInt(testDBIndices);
        buf.flip();
        RPCResponse<?> rp  = null;
        try {
            LogEntry le = new LogEntry(buf,new SyncListener() {
            
                @Override
                public void synced(LogEntry entry) {}
            
                @Override
                public void failed(LogEntry entry, Exception ex) {
                    fail(ex.getMessage());
                }
            },LogEntry.PAYLOAD_TYPE_CREATE);
            le.assignId(viewID, 1L);
            rp = client.replicate(new LSN(viewID,1L), le.serialize(csumAlgo));  
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            csumAlgo.reset();
            if (rp != null) rp.freeBuffers();
        }
        awaitHeartbeat();
    }
    
    private void copyDB() throws Exception {
        ReusableBuffer buf = ReusableBuffer.wrap(
                new byte[(Integer.SIZE/2)+testDB.getBytes().length+copyTestDB.getBytes().length]);
        buf.putInt(1);
        buf.putInt(2);
        buf.putString(testDB);
        buf.putString(copyTestDB);
        buf.flip();
        RPCResponse<?> rp  = null;
        try {
            LogEntry le = new LogEntry(buf,new SyncListener() {
            
                @Override
                public void synced(LogEntry entry) {}
            
                @Override
                public void failed(LogEntry entry, Exception ex) {
                    fail(ex.getMessage());
                }
            },LogEntry.PAYLOAD_TYPE_COPY);
            le.assignId(viewID, 3L);
            rp = client.replicate(new LSN(viewID,3L), le.serialize(csumAlgo));  
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            csumAlgo.reset();
            if (rp != null) rp.freeBuffers();
        }
        awaitHeartbeat();
    }
    
    private void deleteDB() throws Exception {
        ReusableBuffer buf = ReusableBuffer.wrap(new byte[(Integer.SIZE/4)+copyTestDB.getBytes().length]);
        buf.putInt(2);
        buf.putString(copyTestDB);
        buf.flip();
        
        RPCResponse<?> rp  = null;
        try {
            LogEntry le = new LogEntry(buf,new SyncListener() {
            
                @Override
                public void synced(LogEntry entry) {}
            
                @Override
                public void failed(LogEntry entry, Exception ex) {
                    fail(ex.getMessage());
                }
            },LogEntry.PAYLOAD_TYPE_DELETE);
            le.assignId(viewID, 4L);
            rp = client.replicate(new LSN(viewID,4L), le.serialize(csumAlgo));  
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            csumAlgo.reset();
            if (rp != null) rp.freeBuffers();
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
        LogEntry data = new LogEntry(payload , null, LogEntry.PAYLOAD_TYPE_INSERT);
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
        LogEntry data = new LogEntry(payload , null, LogEntry.PAYLOAD_TYPE_INSERT);
        data.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
        
        RPCResponse<?> rp = client.replicate(testLSN, data.serialize(new CRC32()));
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        assertEquals(replicaOperation, (int) mailbox.take());
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
        LogEntry data = new LogEntry(payload , null, LogEntry.PAYLOAD_TYPE_INSERT);
        data.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
        
        RPCResponse<?> rp = client.replicate(testLSN, data.serialize(new CRC32()));
        try {
            rp.get();
        } catch (Exception e) {
            fail("ERROR: "+e.getMessage());
        } finally {
            rp.freeBuffers();
        }
        assertEquals(loadOperation, (int) mailbox.take());
    }
    
    @Override
    public void receiveRecord(ONCRPCRequest rq) {
        int opNum = rq.getRequestHeader().getProcedure();
        if (opNum == heartbeatOperation) {
            heartbeatRequest request = new heartbeatRequest();
            request.unmarshal(new XDRUnmarshaller(rq.getRequestFragment()));
            LSN lsn = new LSN(request.getLsn());
            current = lsn;
            
            rq.sendResponse(new heartbeatResponse());   
        } else if (opNum == replicaOperation) {
            replicaRequest request = new replicaRequest();
            request.unmarshal(new XDRUnmarshaller(rq.getRequestFragment()));
            LSNRange r = request.getRange();
            assertEquals(1, r.getStart().getViewId());
            assertEquals(1, r.getEnd().getViewId());
            assertEquals(replicaRangeLength, 
                    r.getEnd().getSequenceNo() - r.getStart().getSequenceNo());
        } else if (opNum == loadOperation) {
            loadRequest request = new loadRequest();
            request.unmarshal(new XDRUnmarshaller(rq.getRequestFragment()));
            org.xtreemfs.babudb.interfaces.LSN lsn = request.getLsn();
            assertEquals(1, lsn.getViewId());
            assertEquals(2L,lsn.getSequenceNo());
        } else {
            rq.sendException(new errnoException());
            fail("ERROR: received "+opNum);
        }
        mailbox.add(opNum);
    }

    @Override
    public void crashPerformed(Throwable exc) { fail("Slave - client crashed! "+exc.getMessage()); }

    @Override
    public void shutdownPerformed() {}

    @Override
    public void startupPerformed() {}
}
