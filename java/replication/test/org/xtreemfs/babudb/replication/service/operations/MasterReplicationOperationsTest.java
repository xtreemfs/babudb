/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.operations;

import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.TestParameters.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.mock.BabuDBMock;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.service.ReplicationRequestHandler;
import org.xtreemfs.babudb.replication.service.RequestManagement;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;
import org.xtreemfs.babudb.replication.service.accounting.StatesManipulation;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.service.logic.LoadLogic.DBFileMetaDataSet;
import org.xtreemfs.babudb.replication.transmission.ErrorCode;
import org.xtreemfs.babudb.replication.transmission.FileIO;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter.ErrorCodeException;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestControl;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.flease.comm.FleaseMessage.MsgType;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.util.FSUtils;

/**
 * Test of the operation logic for master replication requests.
 * 
 * @author flangner
 * @since 04/08/2011
 */
public class MasterReplicationOperationsTest implements LifeCycleListener {
    
    private static ReplicationConfig    config;
    private static RPCNIOSocketClient   rpcClient;
    private MasterClient                client;
    private RequestDispatcher           dispatcher;
    
    // test data
    private final static Random                random = new Random();
    private final static AtomicReference<LSN>  lastOnView = new AtomicReference<LSN>(new LSN(1,1L));
    private final static String                testFileName = "TestFile";
    private final static long                  maxTestFileSize = 4 * 1024;
    private final static FleaseMessage         testMessage = 
        new FleaseMessage(MsgType.MSG_ACCEPT_ACK);
    private final static LSN                   testLSN = new LSN(8, 15L);
    private final static InetSocketAddress     testAddress = new InetSocketAddress("127.0.0.1", 4711);
    private final static LSN                   rangeStart = new LSN (1, 0L);
    private final static LSN                   rangeEnd = new LSN (1, 2L);
    
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        TimeSync.initializeLocal(TIMESYNC_GLOBAL, TIMESYNC_LOCAL);
        
        config = new ReplicationConfig("config/replication_server0.test", conf0);
        
        FSUtils.delTree(new File(config.getBabuDBConfig().getBaseDir()));
        FSUtils.delTree(new File(config.getBabuDBConfig().getDbLogDir()));
        FSUtils.delTree(new File(config.getTempDir()));
        
        rpcClient = new RPCNIOSocketClient(config.getSSLOptions(), RQ_TIMEOUT, CON_TIMEOUT);
        rpcClient.start();
        rpcClient.waitForStartup();
        
        // create testFile
        FileOutputStream sOut = new FileOutputStream(testFileName);
        
        long size = 0L;
        while (size < maxTestFileSize) {
            sOut.write(random.nextInt());
            size++;
        }
        sOut.flush();
        sOut.close();
        
        // setup flease message
        testMessage.setSender(testAddress);
        testMessage.setCellId(new ASCIIString("testCellId"));
        testMessage.setLeaseHolder(new ASCIIString("testLeaseholder"));
        testMessage.setLeaseTimeout(4711L);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        rpcClient.shutdown();
        rpcClient.waitForShutdown();
        
        TimeSync ts = TimeSync.getInstance();
        ts.shutdown();
        ts.waitForShutdown();
        
        // delete testFile
        File f = new File(testFileName);
        if (!f.delete()) f.deleteOnExit();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        
        client = new ReplicationClientAdapter(rpcClient, config.getInetSocketAddress());
        
        RequestHandler rqHandler = new ReplicationRequestHandler(new StatesManipulation() {
            
            @Override
            public void update(InetSocketAddress participant, LSN acknowledgedLSN, long receiveTime)
                    throws UnknownParticipantException {
                
                assertEquals(testLSN, acknowledgedLSN);
                assertEquals(testAddress, participant);
            }
            
            @Override
            public void requestFinished(SlaveClient slave) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void markAsDead(ClientInterface slave) {
                fail("Operation should not have been accessed by this test!");
            }
        }, new ControlLayerInterface() {
            
            @Override
            public void updateLeaseHolder(InetSocketAddress leaseholder) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void receive(FleaseMessage message) {
                                
                assertEquals(testMessage.getSender(), message.getSender());
                assertEquals(testMessage.getCellId(), message.getCellId());
                assertEquals(testMessage.getLeaseHolder(), message.getLeaseHolder());
                assertEquals(testMessage.getLeaseTimeout(), message.getLeaseTimeout());
            }
            
            @Override
            public void driftDetected() {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void unlockUser() {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void unlockReplication() {
                Logging.logMessage(Logging.LEVEL_INFO, this, "Mock Replication unlocked.");
            }
            
            @Override
            public void registerUserInterface(LockableService service) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void registerReplicationControl(LockableService service) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void registerProxyRequestControl(RequestControl control) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void notifyForSuccessfulFailover(InetSocketAddress master) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void lockAll() throws InterruptedException {
                Logging.logMessage(Logging.LEVEL_INFO, this, "Mock services locked.");
            }
            
            @Override
            public boolean isItMe(InetSocketAddress address) {
                fail("Operation should not have been accessed by this test!");
                return false;
            }
            
            @Override
            public InetSocketAddress getLeaseHolder(int timeout) {
                fail("Operation should not have been accessed by this test!");
                return null;
            }

            @Override
            public void waitForInitialFailover() throws InterruptedException {
                // TODO Auto-generated method stub
                
            }
        }, new BabuDBInterface(new BabuDBMock("BabuDBMock", conf0, testLSN)), new RequestManagement() {
                        
            @Override
            public void enqueueOperation(Object[] args) throws BusyServerException {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void createStableState(LSN lastOnView, InetSocketAddress master, ControlLayerInterface control) {
                
                assertEquals(testLSN, lastOnView);
                assertEquals(testAddress, master);
            }
        }, lastOnView, config.getChunkSize(), new FileIO(config), MAX_Q);
        
        dispatcher = new RequestDispatcher(config);
        dispatcher.setLifeCycleListener(this);
        dispatcher.addHandler(rqHandler);
        dispatcher.start();
        dispatcher.waitForStartup();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        dispatcher.shutdown();
        dispatcher.waitForShutdown();
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testChunkRequest() throws Exception {
        
        // test data
        int minChunkSize = 512;
        
        int start = random.nextInt((int) (maxTestFileSize - minChunkSize));
        int end = random.nextInt((int) (maxTestFileSize - minChunkSize - start)) 
                        + minChunkSize + start;
        ReusableBuffer result = client.chunk(testFileName, start, end).get();
        assertEquals(end - start, result.remaining());
        
        byte[] testData = new byte[end - start];
        FileInputStream sIn = new FileInputStream(testFileName);
        sIn.skip(start);
        sIn.read(testData, 0, end - start);
        sIn.close();
        
        assertEquals(new String(testData), new String(result.array()));
        
        // clean up
        BufferPool.free(result);
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testFleaseRequest() throws Exception {
        
        client.flease(testMessage).get();
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testHeartbeatRequest() throws Exception {
        
        client.heartbeat(testLSN, testAddress.getPort()).get();
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testStateRequest() throws Exception {
        
        LSN result = client.state().get();
        assertEquals(testLSN, result);
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testVolatileStateRequest() throws Exception {
        
        LSN result = client.volatileState().get();
        assertEquals(testLSN, result);
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testTimeRequest() throws Exception {
        long start = TimeSync.getGlobalTime();
        long result = client.time().get();
        long end = TimeSync.getGlobalTime();
        
        assertTrue(result >= start);
        assertTrue(result <= end);
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testLoadRequest() throws Exception {
        DBFileMetaDataSet result = client.load(lastOnView.get()).get();
        assertTrue(result.size() == 0);
        
        result = client.load(testLSN).get();
        
        assertEquals(1, result.size());
        assertEquals(conf0.getBaseDir() + conf0.getDbCfgFile(), result.get(0).file);
        assertEquals(0L, result.get(0).size);
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testReplicaRequest() throws Exception {
        try {
            client.replica(rangeStart, rangeEnd).get();
            fail();
        } catch (ErrorCodeException ee) {
            assertEquals(ErrorCode.LOG_UNAVAILABLE, ee.getCode());
        }
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testSynchronizeRequest() throws Exception {
        client.synchronize(testLSN, testAddress.getPort()).get(); 
    }
        
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() { }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() { }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable cause) {
        fail("Dispatcher crashed: " + cause.getMessage());
    }
}
