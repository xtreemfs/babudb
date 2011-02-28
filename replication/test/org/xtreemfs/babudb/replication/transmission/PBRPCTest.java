/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;


import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Chunk;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.Database;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.DatabaseName;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.HeartbeatMessage;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.SSLOptions;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;

import com.google.protobuf.Message;

import static junit.framework.Assert.*;
import static org.xtreemfs.babudb.replication.TestParameters.mock0Conf;

/**
 * These tests are just a prove of concept and neither do nor will ever probe all RPCs provided by
 * the replication plugin. 
 * 
 * @author flangner
 * @since 02/25/2011
 */
public class PBRPCTest implements LifeCycleListener {

    private final static int RQ_TIMEOUT = 10 * 1000;
    private final static int CON_TIMEOUT = 20 * 60 * 1000;
    private final static int TIMESYNC_GLOBAL = 3 * 1000;
    private final static int TIMESYNC_LOCAL = 3 * 1000;
    
    private RequestDispatcher dispatcher;
    
    private RPCNIOSocketClient client;
    
    private ReplicationConfig config;
    
    private SSLOptions ssl = null;
    
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Logging.start(Logging.LEVEL_ERROR, Category.all);
        TimeSync.initializeLocal(TIMESYNC_GLOBAL, TIMESYNC_LOCAL);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TimeSync ts = TimeSync.getInstance();
        ts.shutdown();
        ts.waitForShutdown();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        config = new ReplicationConfig("config/replication_server0.test", mock0Conf);
        
        dispatcher = new RequestDispatcher(config);
        dispatcher.setLifeCycleListener(this);
        
        client = new RPCNIOSocketClient(ssl, RQ_TIMEOUT, CON_TIMEOUT);
        client.start();
        client.waitForStartup();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        dispatcher.shutdown();
        dispatcher.waitForShutdown();
        
        client.shutdown();
        client.waitForShutdown();
    }
    
    /**
     * @throws Exception
     */
    @Test
    public void testPBRPCClientAdapter() throws Exception {
        
        // test-data
        final String fileName = "testFile";
        final long offsetStart = 815L;
        final long offsetEnd = 4711L;
        final String chunkResult = "chunkResult";
        final int port = 12345;
        final LSN lsn = new LSN(345, 1337L);
        
        // registers operations at the handler
        Map<Integer, Operation> ops = new HashMap<Integer, Operation>();
        ops.put(ReplicationServiceConstants.PROC_ID_HEARTBEAT, new Operation() {
            
            @Override
            public void startRequest(Request rq) {
                HeartbeatMessage hbm = (HeartbeatMessage) rq.getRequestMessage();
                assertEquals(port, hbm.getPort());
                assertEquals(lsn.getViewId(), hbm.getLsn().getViewId());
                assertEquals(lsn.getSequenceNo(), hbm.getLsn().getSequenceNo());
                
                rq.sendSuccess(ErrorCodeResponse.getDefaultInstance());
            }
            
            @Override
            public int getProcedureId() {
                return ReplicationServiceConstants.PROC_ID_HEARTBEAT;
            }
            
            @Override
            public Message getDefaultRequest() {
                return HeartbeatMessage.getDefaultInstance();
            }
        });
        ops.put(ReplicationServiceConstants.PROC_ID_CHUNK, new Operation() {
            
            @Override
            public void startRequest(Request rq) {
                Chunk req = (Chunk) rq.getRequestMessage();
                assertEquals(fileName, req.getFileName());
                assertEquals(offsetStart, req.getStart());
                assertEquals(offsetEnd, req.getEnd());
                
                rq.sendSuccess(ErrorCodeResponse.getDefaultInstance(), 
                        ReusableBuffer.wrap(chunkResult.getBytes()));
            }
            
            @Override
            public int getProcedureId() {
                return ReplicationServiceConstants.PROC_ID_CHUNK;
            }
            
            @Override
            public Message getDefaultRequest() {
                return Chunk.getDefaultInstance();
            }
        });
        dispatcher.addHandler(
                new RequestHandlerMock(ReplicationServiceConstants.INTERFACE_ID, ops));
        dispatcher.start();
        dispatcher.waitForStartup();
        
        // setup the client 
        PBRPCClientAdapter testClient = new PBRPCClientAdapter(client, 
                config.getInetSocketAddress());
        
        // run some test operations
        ReusableBuffer result = testClient.chunk(fileName, offsetStart, offsetEnd).get();
        assertEquals(chunkResult, new String(result.array()));
        
        testClient.heartbeat(lsn, port).get();
    }
    
    /** 
     * @throws Exception
     */
    @Test
    public void testRemoteAccessClient() throws Exception {
        
        // test-data
        final String testDatabaseName = "testDatabase";
        final int testDatabaseID = 4711;
        
        // registers operations at the handler
        Map<Integer, Operation> ops = new HashMap<Integer, Operation>();
        ops.put(RemoteAccessServiceConstants.PROC_ID_GETDATABASE, new Operation() {
            
            @Override
            public void startRequest(Request rq) {
                DatabaseName req = (DatabaseName) rq.getRequestMessage();
                assertEquals(testDatabaseName, req.getDatabaseName());
                
                rq.sendSuccess(Database.newBuilder().setDatabaseId(4711)
                                                    .setDatabaseName(testDatabaseName).build());
            }
            
            @Override
            public int getProcedureId() {
                return RemoteAccessServiceConstants.PROC_ID_GETDATABASE;
            }
            
            @Override
            public Message getDefaultRequest() {
                return DatabaseName.getDefaultInstance();
            }
        });
        dispatcher.addHandler(
                new RequestHandlerMock(RemoteAccessServiceConstants.INTERFACE_ID, ops));
        dispatcher.start();
        dispatcher.waitForStartup();
        
        // setup the client
        RemoteClientAdapter testClient = new RemoteClientAdapter(client);
        
        // run some test operations
        int result = testClient.getDatabase(testDatabaseName, 
                                            config.getInetSocketAddress()).get();
        assertEquals(testDatabaseID, result);
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

    /**
     * Mock for request handler logic.
     * 
     * @author flangner
     * @since 02/25/2011
     */
    private final class RequestHandlerMock extends RequestHandler {

        private final int interfaceID;
        
        RequestHandlerMock(int interfaceID, Map<Integer, Operation> ops) {
            this.interfaceID = interfaceID;
            this.operations.putAll(ops);
        }
        
        /* (non-Javadoc)
         * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#
         *              getInterfaceID()
         */
        @Override
        public int getInterfaceID() {
            return interfaceID;
        }
    }
}
