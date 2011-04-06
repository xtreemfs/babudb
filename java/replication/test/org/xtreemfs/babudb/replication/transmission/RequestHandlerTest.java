/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission;

import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.TestParameters.*;
import static org.xtreemfs.babudb.replication.transmission.TransmissionLayer.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.mock.RequestHandlerMock;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.pbrpc.client.PBRPCException;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;
import org.xtreemfs.foundation.pbrpc.client.RPCResponse;

import com.google.protobuf.Message;

/**
 * Test the queuing capabilities of {@link RequestHandler}.
 * 
 * @author flangner
 * @since 04/06/2011
 */
public class RequestHandlerTest implements LifeCycleListener {

    // test parameters
    // this delay has to be proportional to MAX_Q 
    private final static int MESSAGE_RECEIVE_DELAY = 1000;
    
    private RequestDispatcher dispatcher;
    private RequestControl control;
    private RPCNIOSocketClient client;
    private ReplicationConfig config;
    
    // test data
    private final int interfaceId = 815;
    private final int operationId = 4711;
    private final Operation operation = new Operation() {
        
        @Override
        public void startRequest(Request rq) {
            
            // simply echo the request
            ErrorCodeResponse request = (ErrorCodeResponse) rq.getRequestMessage();
            assertTrue(request.getErrorCode() != -1);
            rq.sendSuccess(request);
        }
        
        @Override
        public int getProcedureId() {
            return operationId;
        }
        
        @Override
        public Message getDefaultRequest() {
            return ErrorCodeResponse.getDefaultInstance();
        }
    };
    
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
        config = new ReplicationConfig("config/replication_server0.test", conf0);
        dispatcher = new RequestDispatcher(config);
        dispatcher.setLifeCycleListener(this);
        
        Map<Integer, Operation> testOps = new HashMap<Integer, Operation>();
        testOps.put(operationId, operation);
        RequestHandler handler = new RequestHandlerMock(MAX_Q, interfaceId, testOps);
        dispatcher.addHandler(handler);
        
        control = handler;
        dispatcher.start();
        dispatcher.waitForStartup();
        
        client = new RPCNIOSocketClient(config.getSSLOptions(), RQ_TIMEOUT, CON_TIMEOUT);
        client.start();
        client.waitForStartup();
    }
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        client.shutdown();
        client.waitForShutdown();
        
        dispatcher.shutdown();
        dispatcher.waitForShutdown();
    }

    /** 
     * Default message processing test sending without queuing (simplified variant of PBRPCTest).
     * 
     * @throws Exception
     */
    @Test
    public void testWithoutQueuing() throws Exception {
        
        // test data
        final int numMessagesToSend = MAX_Q + 10;
        
        for (int i = 0; i < numMessagesToSend; i++) {
            assertEquals(i, send(
                    ErrorCodeResponse.newBuilder().setErrorCode(i).build()).get().getErrorCode());
        }
    }

    /** 
     * Tests message processing after queuing the messages for a while.
     * Tests also if messages are being dumped correctly if queue limit exceeds.
     * 
     * @throws Exception
     */
    @Test
    public void testQueuing() throws Exception {
                
        // test data
        final int numMessagesToDump = 10;
        
        // to not modify
        final int numMessagesToSend = MAX_Q + numMessagesToDump;
        
        // test queue limit
        control.enableQueuing();
        
        List<RPCResponse<ErrorCodeResponse>> rps = 
            new ArrayList<RPCResponse<ErrorCodeResponse>>(numMessagesToSend);
        for (int i = 0; i < numMessagesToSend; i++) {
            rps.add(i, send(ErrorCodeResponse.newBuilder().setErrorCode(i).build()));
        }
        
        // need to wait a bit until the messages have arrived at the dispatcher
        Thread.sleep(MESSAGE_RECEIVE_DELAY);
        
        control.processQueue();
        
        for (int i = 0; i < numMessagesToDump; i++) {
            try {
                rps.get(i).get();
                fail("message should not have been queued (limit exceeded)");
            } catch (Exception e) {
                if (!(e instanceof PBRPCException)) {
                    fail("Unexpected exception caught: " + e.toString());
                }
            }
        }
        
        for (int i = numMessagesToDump; i < numMessagesToSend; i++) {
            assertEquals(i, rps.get(i).get().getErrorCode());
        }
    }
    
    /** 
     * Half of the messages send used to be expired.
     * 
     * @throws Exception
     */
    @Test
    public void testQueuedMessageExpiring() throws Exception {
        
        // test data
        final int numMessagesToSend = MAX_Q;
        assertTrue(numMessagesToSend <= MAX_Q);
        
        // test message expiration
        control.enableQueuing();
        
        List<RPCResponse<ErrorCodeResponse>> rps = 
            new ArrayList<RPCResponse<ErrorCodeResponse>>(numMessagesToSend);
        for (int i = 0; i < (numMessagesToSend / 2); i++) {
            rps.add(i, send(ErrorCodeResponse.newBuilder().setErrorCode(-1).build()));
        }
        
        // get a delay between messages to drop because of expiration and messages to process
        Thread.sleep(RQ_TIMEOUT / 2);
        
        for (int i = (numMessagesToSend / 2); i < numMessagesToSend; i++) {
            rps.add(i, send(ErrorCodeResponse.newBuilder().setErrorCode(i).build()));
        }
        
        // wait until the first bunch of messages gets expired
        Thread.sleep(RQ_TIMEOUT / 2 + MESSAGE_RECEIVE_DELAY);
        
        control.processQueue();
        
        for (int i = 0; i < (numMessagesToSend / 2); i++) {
            try {
                rps.get(i).get();
                fail("message should not have been queued (limit exceeded)");
            } catch (Exception e) {                
                if (!(e instanceof IOException)) {
                    fail("Unexpected exception caught: " + e.toString());
                }
            }
        }
        
        for (int i = (numMessagesToSend / 2); i < numMessagesToSend; i++) {
            assertEquals(i, rps.get(i).get().getErrorCode());
        }
    }
    
    /**
     * Sends a message to the dispatcher.
     * 
     * @param message
     * @return future for the response.
     */
    private RPCResponse<ErrorCodeResponse> send(Message message) {
        
        RPCResponse<ErrorCodeResponse> result = 
            new RPCResponse<ErrorCodeResponse>(ErrorCodeResponse.getDefaultInstance());
        client.sendRequest(config.getInetSocketAddress(), AUTHENTICATION, USER_CREDENTIALS, 
                interfaceId, operationId, message, null, result, false);
        
        return result;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#startupPerformed()
     */
    @Override
    public void startupPerformed() { /* ignored */ }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#shutdownPerformed()
     */
    @Override
    public void shutdownPerformed() { /* ignored */ }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.LifeCycleListener#crashPerformed(java.lang.Throwable)
     */
    @Override
    public void crashPerformed(Throwable cause) { fail(cause.getMessage()); }
}
