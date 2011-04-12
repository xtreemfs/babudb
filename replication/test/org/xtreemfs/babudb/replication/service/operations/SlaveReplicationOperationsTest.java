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

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.mock.BabuDBMock;
import org.xtreemfs.babudb.mock.StatesManipulationMock;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.LockableService;
import org.xtreemfs.babudb.replication.LockableService.ServiceLockedException;
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.service.ReplicationRequestHandler;
import org.xtreemfs.babudb.replication.service.RequestManagement;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.service.ReplicationStage.BusyServerException;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;
import org.xtreemfs.babudb.replication.transmission.FileIO;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestControl;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;

/**
 * Test of the operation logic for slave replication requests.
 * 
 * @author flangner
 * @since 04/08/2011
 */
public class SlaveReplicationOperationsTest implements LifeCycleListener {
    
    private static ReplicationConfig    config;
    private static RPCNIOSocketClient   rpcClient;
    private SlaveClient                 client;
    private RequestDispatcher           dispatcher;
    
    // test data
    private static final AtomicReference<LSN> lastOnView = new AtomicReference<LSN>(new LSN(1,1L));
    private static final LSN testLSN = new LSN(8, 15L);
    private static final byte testType = LogEntry.PAYLOAD_TYPE_INSERT;
    private static final byte[] testPayload = "testData".getBytes();
    private static final LogEntry testEntry = new LogEntry(ReusableBuffer.wrap(testPayload), null, testType);
    
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        TimeSync.initializeLocal(TIMESYNC_GLOBAL, TIMESYNC_LOCAL);
        
        config = new ReplicationConfig("config/replication_server0.test", conf0);
        
        rpcClient = new RPCNIOSocketClient(config.getSSLOptions(), RQ_TIMEOUT, CON_TIMEOUT);
        rpcClient.start();
        rpcClient.waitForStartup();
        testEntry.assignId(testLSN.getViewId(), testLSN.getSequenceNo());
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
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        
        client = new ReplicationClientAdapter(rpcClient, config.getInetSocketAddress());
        
        RequestHandler rqHandler = new ReplicationRequestHandler(
                new StatesManipulationMock(config.getInetSocketAddress()), 
                new ControlLayerInterface() {
            
            @Override
            public void updateLeaseHolder(InetSocketAddress leaseholder) throws Exception {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void receive(FleaseMessage message) {
                fail("Operation should not have been accessed by this test!");
                
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
                fail("Operation should not have been accessed by this test!");
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
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public boolean isItMe(InetSocketAddress address) {
                fail("Operation should not have been accessed by this test!");
                return false;
            }
            
            @Override
            public InetSocketAddress getLeaseHolder() {
                fail("Operation should not have been accessed by this test!");
                return null;
            }
        }, new BabuDBInterface(new BabuDBMock("BabuDBMock", conf0)), new RequestManagement() {
            
            @Override
            public void finalizeRequest(StageRequest op) {
                fail("Operation should not have been accessed by this test!");
            }
            
            @Override
            public void enqueueOperation(Object[] args) throws BusyServerException, ServiceLockedException {
                
                assertTrue(args.length == 2);
                LSN receivedLSN = (LSN) args[0];
                assertEquals(testLSN, receivedLSN);
                
                LogEntry receivedEntry = (LogEntry) args[1];
                assertEquals(testEntry.getPayloadType(), receivedEntry.getPayloadType());
                assertEquals(testType, receivedEntry.getPayloadType());
                assertEquals(testEntry.getPayload().capacity(), receivedEntry.getPayload().capacity());
                assertEquals(testEntry.getPayload().remaining(), receivedEntry.getPayload().remaining());
                assertEquals(testEntry.getPayload().position(), receivedEntry.getPayload().position());
                assertEquals(new String(testEntry.getPayload().array()), 
                             new String(receivedEntry.getPayload().array()));
                
                // clean up
                receivedEntry.free();
            }
            
            @Override
            public void createStableState(LSN lastOnView, InetSocketAddress master) {
                fail("Operation should not have been accessed by this test!");
            }
        }, lastOnView, config.getChunkSize(), new FileIO(config), MAX_Q);
        
        rqHandler.processQueue();
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
    public void testReplicateRequest() throws Exception {
        
        // serialize the request
        Checksum csumAlgo = new CRC32();
        ReusableBuffer data = testEntry.serialize(csumAlgo);
        csumAlgo.reset();
        
        client.replicate(testLSN, data).get();
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
