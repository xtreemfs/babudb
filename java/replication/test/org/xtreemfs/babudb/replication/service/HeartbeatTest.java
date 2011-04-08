/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.mock.RequestHandlerMock;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.ErrorCodeResponse;
import org.xtreemfs.babudb.pbrpc.GlobalTypes.HeartbeatMessage;
import org.xtreemfs.babudb.replication.proxy.ProxyAccessClient;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
import org.xtreemfs.babudb.replication.transmission.client.ClientFactory;
import org.xtreemfs.babudb.replication.transmission.client.ReplicationClientAdapter;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestDispatcher;
import org.xtreemfs.foundation.LifeCycleListener;
import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;
import org.xtreemfs.foundation.pbrpc.client.RPCNIOSocketClient;

import com.google.protobuf.Message;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;
import static org.xtreemfs.babudb.replication.TestParameters.*;

/**
 * Tests for the behavior of {@link HeartbeatThread}.
 * 
 * @author flangner
 * @since 04/06/2011
 */

public class HeartbeatTest implements LifeCycleListener {

    // test parameters
    private final static int BASIC_PORT = 12345;
    private final static long MAX_DELAY_BETWEEN_HBS_ALLOWED = 
        HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS + 200L;
    
    private RequestDispatcher   dispatcher;
    private HeartbeatThread     hbt;
    private RPCNIOSocketClient  client;
    private ReplicationConfig   config;
    
    // test data
    private final static AtomicReference<LSN> lsn = new AtomicReference<LSN>(new LSN(0, 0L));
    private final static AtomicLong lastHeartbeat = 
        new AtomicLong(Long.MAX_VALUE - MAX_DELAY_BETWEEN_HBS_ALLOWED);
    
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
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        
        config = new ReplicationConfig("config/replication_server0.test", conf0);
        dispatcher = new RequestDispatcher(config);
        dispatcher.setLifeCycleListener(this);
        
        // registers heartbeat operation at the handler
        Map<Integer, Operation> ops = new HashMap<Integer, Operation>();
        ops.put(ReplicationServiceConstants.PROC_ID_HEARTBEAT, new Operation() {
                        
            @Override
            public int getProcedureId() {
                return ReplicationServiceConstants.PROC_ID_HEARTBEAT;
            }
            
            @Override
            public Message getDefaultRequest() {
                return HeartbeatMessage.getDefaultInstance();
            }

            @Override
            public void processRequest(Request rq) {
                HeartbeatMessage hbm = (HeartbeatMessage) rq.getRequestMessage();
                
                long currentTimestamp = System.currentTimeMillis();
                long oldTimestamp = lastHeartbeat.getAndSet(currentTimestamp);
                assertTrue(currentTimestamp < (oldTimestamp + MAX_DELAY_BETWEEN_HBS_ALLOWED));
                
                synchronized (lsn) {
                    assertEquals(BASIC_PORT, hbm.getPort());
                    assertEquals(lsn.get().getViewId(), hbm.getLsn().getViewId());
                    assertEquals(lsn.get().getSequenceNo(), hbm.getLsn().getSequenceNo());   
                }
                
                rq.sendSuccess(ErrorCodeResponse.getDefaultInstance());
            }
        });
        dispatcher.addHandler(new RequestHandlerMock(MAX_Q, 
                ReplicationServiceConstants.INTERFACE_ID, ops));
        dispatcher.start();
        dispatcher.waitForStartup();
        
        client = new RPCNIOSocketClient(config.getSSLOptions(), RQ_TIMEOUT, CON_TIMEOUT);
        client.start();
        client.waitForStartup();
        
        // generate list of participants
        int numOfParticipants = new Random().nextInt(MAX_PARTICIPANTS) + MIN_PARTICIPANTS;
        Set<InetSocketAddress> participants = new HashSet<InetSocketAddress>();
        for (int i = 1; i < numOfParticipants; i++) {
            participants.add(new InetSocketAddress(BASIC_PORT + numOfParticipants));
        }
        participants.add(config.getInetSocketAddress());
        ParticipantsOverview states = new ParticipantsStates(0, participants, new ClientFactory() {
            
            @Override
            public ProxyAccessClient getProxyClient() {
                fail("Generation of RemoteAccessClients is not supported by this test.");
                return null;
            }
            
            @Override
            public ReplicationClientAdapter getClient(InetSocketAddress receiver) {
                return new ReplicationClientAdapter(client, receiver);
            }
        });
        
        hbt = new HeartbeatThread(states, BASIC_PORT);
        hbt.start(new LSN(0,0L));
        hbt.waitForStartup();
    }

    /** 
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        hbt.shutdown();
        hbt.waitForShutdown();
        
        client.shutdown();
        client.waitForShutdown();
        
        dispatcher.shutdown();
        dispatcher.waitForShutdown();
    }

    /** 
     * Tests basic heartbeat functionality and infarction feature.
     * Typical scenario of the HBT usage.
     * 
     * @throws Exception
     */
    @Test
    public void testInfarctAndUpdate() throws Exception {
        
        // let the hbt do some work
        Thread.sleep(3 * HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS);
        
        // infarct the hbt and invalidate the expected lsn
        hbt.infarction();
        Thread.sleep(HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS);
        synchronized (lsn) {
            lsn.set(new LSN(-1, -1L));
            lastHeartbeat.set(0);
        }
        
        // remain a few beats idle, just to make sure the HBT has a flatline
        Thread.sleep(3 * HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS);
        
        // update the hbt and re-animate it too
        lsn.set(new LSN(1, 1L));
        lastHeartbeat.set(Long.MAX_VALUE - MAX_DELAY_BETWEEN_HBS_ALLOWED);
        hbt.updateLSN(new LSN(1, 1L));
        
        // again let it do some work
        Thread.sleep(3 * HeartbeatThread.MAX_DELAY_BETWEEN_HEARTBEATS);
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
